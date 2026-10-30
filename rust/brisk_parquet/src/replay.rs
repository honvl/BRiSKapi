//! A broker's book changes (`schema.BOOK_CHANGES` in `briskapi/schema.py`), replayed: each issue's four books, kept as its
//! quotes' changes apply, must give the quotes' own best levels, and the depths table's checkpoints (an issue's whole books,
//! now and then) are where `Columns.checkpoints` in `briskapi/_parquet.py` puts them, with what it puts there.
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, UInt8Type, UInt32Type};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, ListArray, RecordBatch, StructArray, UInt8Array, UInt32Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};

use crate::error::{Failure, Res, refuse};
use crate::require;
use crate::schema::{self, BOOKS, CHANGES, CHECKPOINT, LEVELS, MAX_BOOK_LEVELS};
use crate::units::{tenths, tenths_raw, whole, whole_raw};

/// A book: price10 to (quantity, orders).
pub type Side = BTreeMap<u64, (u64, u32)>;

/// An issue's four books, in `BOOKS` order.
#[derive(Default)]
pub struct Books {
    pub sides: [Side; 4],
}

impl Books {
    /// Every level, as (book, price10, quantity, orders): the books in order, each best price first (bids fall, asks rise),
    /// until `each` says to stop. Whether it did not.
    pub fn each(&self, mut each: impl FnMut(u8, u64, u64, u32) -> bool) -> bool {
        fn run<'a>(
            book: u8,
            mut levels: impl Iterator<Item = (&'a u64, &'a (u64, u32))>,
            each: &mut impl FnMut(u8, u64, u64, u32) -> bool,
        ) -> bool {
            levels.all(|(&price, &(quantity, orders))| each(book, price, quantity, orders))
        }
        for (book, side) in self.sides.iter().enumerate() {
            let ok = if BOOKS[book].1 {
                run(book as u8, side.iter().rev(), &mut each)
            } else {
                run(book as u8, side.iter(), &mut each)
            };
            if !ok {
                return false;
            }
        }
        true
    }
}

fn bad_schema(name: &str) -> Failure {
    Failure::Refused(format!("Unexpected schema in {name}"))
}

/// A list column of book changes (`changes`, or `checkpoint`, which has nulls) as slices.
pub struct ChangeList<'a> {
    offsets: &'a [i32],
    nulls: Option<&'a NullBuffer>,
    book: &'a [u8],
    price: &'a [i128],
    size: &'a [i128],
    count: &'a [u32],
}

impl<'a> ChangeList<'a> {
    fn of(batch: &'a RecordBatch, name: &str) -> Res<Self> {
        let list = batch
            .column_by_name(name)
            .and_then(|c| c.as_list_opt::<i32>())
            .ok_or_else(|| bad_schema(name))?;
        let items = list
            .values()
            .as_struct_opt()
            .ok_or_else(|| bad_schema(name))?;
        let columns = (
            items.column(0).as_primitive_opt::<UInt8Type>(),
            items.column(1).as_primitive_opt::<Decimal128Type>(),
            items.column(2).as_primitive_opt::<Decimal128Type>(),
            items.column(3).as_primitive_opt::<UInt32Type>(),
        );
        let (Some(book), Some(price), Some(size), Some(count)) = columns else {
            return Err(bad_schema(name));
        };
        Ok(ChangeList {
            offsets: list.value_offsets(),
            nulls: list.nulls(),
            book: book.values(),
            price: price.values(),
            size: size.values(),
            count: count.values(),
        })
    }

    fn range(&self, row: usize) -> (usize, usize) {
        (self.offsets[row] as usize, self.offsets[row + 1] as usize)
    }
}

/// A quote's own book side, as slices: the levels of each row between two offsets.
struct BookList<'a> {
    offsets: &'a [i32],
    price: &'a [i128],
    size: &'a [i128],
    count: &'a [u32],
}

impl<'a> BookList<'a> {
    fn of(batch: &'a RecordBatch, name: &str) -> Res<Self> {
        let list = batch
            .column_by_name(name)
            .and_then(|c| c.as_list_opt::<i32>())
            .ok_or_else(|| bad_schema(name))?;
        let items = list
            .values()
            .as_struct_opt()
            .ok_or_else(|| bad_schema(name))?;
        let (Some(price), Some(size), Some(count)) = (
            items.column(0).as_primitive_opt::<Decimal128Type>(),
            items.column(1).as_primitive_opt::<Decimal128Type>(),
            items.column(2).as_primitive_opt::<UInt32Type>(),
        ) else {
            return Err(bad_schema(name));
        };
        Ok(BookList {
            offsets: list.value_offsets(),
            price: price.values(),
            size: size.values(),
            count: count.values(),
        })
    }
}

impl BookList<'_> {
    /// Whether the quote's levels `from..to` are the first LEVELS of `levels`, best first, and no more.
    fn same<'a>(
        &self,
        levels: impl Iterator<Item = (&'a u64, &'a (u64, u32))>,
        from: usize,
        to: usize,
    ) -> bool {
        let mut at = from;
        let same = levels.take(LEVELS).all(|(&price, &(quantity, orders))| {
            let ok = at < to
                && self.price[at] == tenths_raw(price)
                && self.size[at] == whole_raw(quantity)
                && self.count[at] == orders;
            at += 1;
            ok
        });
        same && at == to
    }
}

/// The columns of a depths batch that hold book changes: its quotes' changes and books, and its checkpoints.
pub struct Rows<'a> {
    changes: ChangeList<'a>,
    books: [BookList<'a>; 4],
    checkpoint: ChangeList<'a>,
}

impl<'a> Rows<'a> {
    pub fn new(batch: &'a RecordBatch) -> Res<Self> {
        Ok(Rows {
            changes: ChangeList::of(batch, CHANGES)?,
            books: [
                BookList::of(batch, BOOKS[0].0)?,
                BookList::of(batch, BOOKS[1].0)?,
                BookList::of(batch, BOOKS[2].0)?,
                BookList::of(batch, BOOKS[3].0)?,
            ],
            checkpoint: ChangeList::of(batch, CHECKPOINT)?,
        })
    }

    /// Whether row `row`'s checkpoint is what it should be: null if `point` is false, else the books' levels as `Books::each`
    /// gives them.
    pub fn checkpoint_is(&self, row: usize, books: &Books, point: bool) -> bool {
        let cell = &self.checkpoint;
        let null = cell.nulls.is_some_and(|n| n.is_null(row));
        if !point || null {
            return !point && null;
        }
        let (lo, hi) = cell.range(row);
        let mut at = lo;
        let same = books.each(|book, price, quantity, orders| {
            let ok = at < hi
                && cell.book[at] == book
                && cell.price[at] == tenths_raw(price)
                && cell.size[at] == whole_raw(quantity)
                && cell.count[at] == orders;
            at += 1;
            ok
        });
        same && at == hi
    }
}

/// An issue's books as its rows' changes have left them, and how many changes since its last checkpoint.
#[derive(Default)]
pub struct IssueState {
    pub books: Books,
    count: i64,
    started: bool,
}

impl IssueState {
    /// Apply row `row`'s book changes, which must be valid, must not empty a level the book does not have, and must give the
    /// quote's own best levels (`schema.py`'s `_changes`, `apply_changes` and the replay check of `_scan`). Whether the row
    /// makes a checkpoint: an issue's first row holds its books whole and counts none, every `checkpoint_changes()` changes
    /// after it do.
    pub fn apply(&mut self, rows: &Rows, row: usize) -> Res<bool> {
        let list = &rows.changes;
        let (lo, hi) = list.range(row);
        require!(
            hi - lo <= BOOKS.len() * MAX_BOOK_LEVELS,
            "Invalid book changes"
        );
        let mut last: Option<(u8, u64)> = None;
        for at in lo..hi {
            let (Some(price), Some(size)) = (tenths(list.price[at]), whole(list.size[at])) else {
                return refuse("Invalid decimal value");
            };
            let (book, orders) = (list.book[at], list.count[at]);
            require!(
                (book as usize) < BOOKS.len() && price > 0 && (size == 0) == (orders == 0),
                "Invalid book change"
            );
            require!(
                last.is_none_or(|(before, was)| book > before
                    || (book == before
                        && if BOOKS[book as usize].1 {
                            price < was
                        } else {
                            price > was
                        })),
                "Book changes out of order"
            );
            last = Some((book, price));
        }
        for at in lo..hi {
            let (price, size) = (
                tenths(list.price[at]).unwrap_or(0),
                whole(list.size[at]).unwrap_or(0),
            );
            let side = &mut self.books.sides[list.book[at] as usize];
            if size != 0 {
                side.insert(price, (size, list.count[at]));
            } else {
                require!(
                    side.remove(&price).is_some(),
                    "Book change empties a level the book does not have"
                );
            }
        }
        require!(
            self.books
                .sides
                .iter()
                .all(|side| side.len() <= MAX_BOOK_LEVELS),
            "Invalid book changes"
        );
        for (book, side) in self.books.sides.iter().enumerate() {
            let own = &rows.books[book];
            let (from, to) = (own.offsets[row] as usize, own.offsets[row + 1] as usize);
            let same = if BOOKS[book].1 {
                own.same(side.iter().rev(), from, to)
            } else {
                own.same(side.iter(), from, to)
            };
            require!(same, "Book changes do not give the quote's book");
        }
        let n = (hi - lo) as i64;
        if !self.started {
            (self.started, self.count) = (true, -n);
        }
        self.count += n;
        if self.count >= schema::checkpoint_changes() {
            self.count = 0;
            return Ok(true);
        }
        Ok(false)
    }
}

/// The lists of a `changes`-typed column built row by row: some null, some books' levels whole.
#[derive(Default)]
pub struct Lists {
    offsets: Vec<i32>,
    valid: Vec<bool>,
    book: Vec<u8>,
    price: Vec<i128>,
    size: Vec<i128>,
    count: Vec<u32>,
}

impl Lists {
    pub fn new() -> Self {
        Lists {
            offsets: vec![0],
            ..Default::default()
        }
    }

    pub fn null(&mut self) {
        self.offsets.push(self.book.len() as i32);
        self.valid.push(false);
    }

    /// A row holding all of `books`' levels.
    pub fn books(&mut self, books: &Books) {
        books.each(|book, price, quantity, orders| {
            self.book.push(book);
            self.price.push(tenths_raw(price));
            self.size.push(whole_raw(quantity));
            self.count.push(orders);
            true
        });
        self.offsets.push(self.book.len() as i32);
        self.valid.push(true);
    }

    pub fn finish(self) -> Res<ArrayRef> {
        let decimals = |values: Vec<i128>| -> Res<ArrayRef> {
            Ok(Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 16)
                    .map_err(Failure::from)?,
            ))
        };
        let items = StructArray::new(
            schema::change_fields(),
            vec![
                Arc::new(UInt8Array::from(self.book)),
                decimals(self.price)?,
                decimals(self.size)?,
                Arc::new(UInt32Array::from(self.count)),
            ],
            None,
        );
        let field = Arc::new(Field::new(
            "item",
            DataType::Struct(schema::change_fields()),
            false,
        ));
        let nulls = self
            .valid
            .contains(&false)
            .then(|| NullBuffer::from(self.valid));
        Ok(Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(self.offsets)),
            Arc::new(items),
            nulls,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn books_are_listed_best_first_book_by_book() {
        let mut books = Books::default();
        for (book, price) in [(0usize, 99u64), (0, 100), (1, 101), (1, 102), (2, 50)] {
            books.sides[book].insert(price, (price * 10, 1));
        }
        let mut seen = Vec::new();
        assert!(books.each(|book, price, _, _| {
            seen.push((book, price));
            true
        }));
        assert_eq!(seen, [(0, 100), (0, 99), (1, 101), (1, 102), (2, 50)]);
        let mut count = 0;
        assert!(!books.each(|_, _, _, _| {
            count += 1;
            count < 3
        }));
        assert_eq!(count, 3);
    }
}
