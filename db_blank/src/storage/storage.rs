#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(dead_code)]
// this file is auto-generated see make_blank.py
#![allow(dead_code)]
use libc::statvfs;
use core::f32;
use std::alloc::{Layout, alloc, dealloc};
use std::ffi::CString;
use std::slice;
use itertools::Itertools;
use rand::{Rng, RngExt, distr::Alphanumeric, rngs::ThreadRng};
use std::cmp::{Reverse, max};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt::{self},
    hash::Hash,
    u8,
};

use ordered_float::OrderedFloat;

// === Part 1.1 setup ===

// #[derive] tells the compiler to derive functions to satisfy the traits we list.
// For example Debug lets us print the struct with println!("{}", struct)
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct Schema {
    table_name: String,
    columns: Vec<SchemaColumn>,
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct SchemaColumn {
    pub name: String,
    pub col_kind: ColKind,
    pub col_type: ColType,
}
// we are going to use a reduced set of types
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum ColType {
    Bool = 0,
    Int = 1,
    Float = 2,
    String = 3,
    Blob = 4,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum ColKind {
    PrimaryKey = 0,
    UniqueIndex = 1,
    NonUniqueIndex = 2,
    // the only kind of column that can be null. Some DBMSes support nulls in indexed cols but for simplicity we won't
    Nullable = 3,
    Normal = 4,
}

// === Part 1.1 Schema ===

impl Schema {
    // IMPLEMENT_ME
    pub fn new(cols: Vec<SchemaColumn>, table_name: String) -> Self {
        panic!("nyi");
    }

    // IMPLEMENT_ME
    pub fn columns(self) -> Vec<SchemaColumn> {
        panic!("nyi");
    }

    pub fn name(&self) -> String {
        return self.table_name.clone();
    }

    // IMPLEMENT_ME
    pub fn primary_key_col_index(&self) -> usize {
        panic!("nyi");
    }

}

#[cfg(test)]
mod part_1_1_schema_tests {
    // allows us to import all of the cool stuff we built above
    use super::*;

    #[test]
    fn test_make_schema() {
        let _schema = Schema::new(
            vec![
                SchemaColumn {
                    name: "key".to_owned(),
                    col_kind: ColKind::PrimaryKey,
                    col_type: ColType::Int,
                },
                SchemaColumn {
                    name: "value".to_owned(),
                    col_kind: ColKind::Nullable,
                    col_type: ColType::String,
                },
            ],
            "my_table".to_owned(),
        );
    }

    // This says we expect the test to panic with the following message.
    // If this were a real system, we should use a Result and expect a specific Err type
    // https://doc.rust-lang.org/std/result/.
    // But I'm going to keep it simple to better teach database concepts.
    #[test]
    #[should_panic(expected = "Cannot have multiple primary keys")]
    fn test_incorrect_schema() {
        Schema::new(
            vec![
                SchemaColumn {
                    name: "key".to_owned(),
                    col_kind: ColKind::PrimaryKey,
                    col_type: ColType::Int,
                },
                SchemaColumn {
                    name: "value".to_owned(),
                    col_kind: ColKind::PrimaryKey,
                    col_type: ColType::String,
                },
            ],
            "my_table".to_owned(),
        );
    }
}

// === Part 1.2 helper functions ===

// returns the u64 and the rest of the bytes
pub fn deserialize_u64(bytes: &[u8]) -> (u64, &[u8]) {
    let new_bytes: [u8; size_of::<u64>()] = bytes[0..size_of::<u64>()].try_into().unwrap();
    // Note that from_be_bytes will do a copy.
    // If we wanted to avoid a copy and just re-interpret the bytes we could use unsafe code
    // with transmute.
    return (u64::from_be_bytes(new_bytes), &bytes[size_of::<u64>()..]);
}

pub fn deserialize_u16(bytes: &[u8]) -> (u16, &[u8]) {
    let new_bytes: [u8; size_of::<u16>()] = bytes[0..size_of::<u16>()].try_into().unwrap();
    return (u16::from_be_bytes(new_bytes), &bytes[size_of::<u16>()..]);
}

pub fn serialize_u64(u: u64) -> [u8; size_of::<u64>()] {
    u64::to_be_bytes(u)
}

pub fn serialize_u16(u: u16) -> [u8; size_of::<u16>()] {
    u16::to_be_bytes(u)
}

// this lets us convert from u8 to ColKind
impl std::convert::TryFrom<u8> for ColKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ColKind::PrimaryKey),
            1 => Ok(ColKind::UniqueIndex),
            2 => Ok(ColKind::NonUniqueIndex),
            3 => Ok(ColKind::Nullable),
            4 => Ok(ColKind::Normal),
            _ => Err(()),
        }
    }
}

impl std::convert::TryFrom<u8> for ColType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ColType::Bool),
            1 => Ok(ColType::Int),
            2 => Ok(ColType::Float),
            3 => Ok(ColType::String),
            4 => Ok(ColType::Blob),
            _ => Err(()),
        }
    }
}

// === Part 1.2 Schema Serialization ===

// IMPLEMENT_ME
pub fn serialize_schema(schema: &Schema) -> Vec<u8> {
    panic!("nyi");
}

// IMPLEMENT_ME
pub fn deserialize_schema(bytes: &[u8]) -> (Schema, &[u8]) {
    panic!("nyi");
}

// HELPFUL this could be helpful but not required
fn serialize_string(s: &String) -> Vec<u8> {
    panic!("nyi");
}

// HELPFUL this could be helpful but not required
fn deserialize_string(bytes: &[u8]) -> (String, &[u8]) {
    panic!("nyi");
}

// HELPFUL this could be helpful but not required
fn serialize_schema_col(col: &SchemaColumn) -> Vec<u8> {
    panic!("nyi");
}

// HELPFUL this could be helpful but not required
fn deserialize_schema_col(bytes: &[u8]) -> (SchemaColumn, &[u8]) {
    panic!("nyi");
}

// test utilities 
const MAX_RANDOM_STRING_LENGTH: usize = 40;
pub fn random_string(rng: &mut ThreadRng) -> String {
    let len: usize = rng.random_range(1..=MAX_RANDOM_STRING_LENGTH);

    return rng
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect();
}

impl ColType {
    pub fn random(rng: &mut ThreadRng) -> Self {
        match rng.random_range(0..=4) {
            0 => ColType::Bool,
            1 => ColType::Int,
            2 => ColType::Float,
            3 => ColType::String,
            4 => ColType::Blob,
            _ => unreachable!(),
        }
    }
}

impl ColKind {
    fn random_excluding_pk(rng: &mut ThreadRng) -> Self {
        match rng.random_range(1..=4) {
            1 => ColKind::UniqueIndex,
            2 => ColKind::NonUniqueIndex,
            3 => ColKind::Nullable,
            4 => ColKind::Normal,
            _ => unreachable!(),
        }
    }
}

const MAX_RANDOM_SCHEMA_COLUMNS: usize = 6;

pub fn make_random_schema(rng: &mut ThreadRng) -> Schema {
    let table_name = random_string(rng);
    let len: usize = rng.random_range(1..=MAX_RANDOM_SCHEMA_COLUMNS);

    let mut columns = Vec::new();
    let mut existing_col_names = HashSet::new();
    for _ in 0..len {

        // make sure column names are unique
        let mut name = random_string(rng);
        while existing_col_names.contains(&name) {
            name = random_string(rng);
        }
        existing_col_names.insert(name.clone());
        
        let schema_col = SchemaColumn{
            name,
            col_type: ColType::random(rng),
            col_kind: ColKind::random_excluding_pk(rng),
        };
        columns.push(schema_col);
    }

    // make sure we have exactly 1 primary key column
    let pk_column: usize = rng.random_range(0..len);
    columns[pk_column].col_kind = ColKind::PrimaryKey;

    return Schema::new( columns, table_name);
}

#[cfg(test)]
mod part_1_2_schema_serialization_tests {
    // allows us to import all of the cool stuff we built above
    use super::*;

    #[test]
    fn test_serialize_schema() {
        let in_schema = Schema::new(
            vec![
                SchemaColumn {
                    name: "key".to_owned(),
                    col_kind: ColKind::PrimaryKey,
                    col_type: ColType::Int,
                },
                SchemaColumn {
                    name: "value".to_owned(),
                    col_kind: ColKind::Nullable,
                    col_type: ColType::String,
                },
                SchemaColumn {
                    name: "indexed_col".to_owned(),
                    col_kind: ColKind::UniqueIndex,
                    col_type: ColType::Float,
                },
                SchemaColumn {
                    name: "foo".to_owned(),
                    col_kind: ColKind::Normal,
                    col_type: ColType::Blob,
                },
                SchemaColumn {
                    name: "bar".to_owned(),
                    col_kind: ColKind::NonUniqueIndex,
                    col_type: ColType::Bool,
                },
            ],
            "my_table".to_owned(),
        );
        let bytes = serialize_schema(&in_schema);
        let (out_schema, left_over_bytes) = deserialize_schema(&bytes);

        assert_eq!(left_over_bytes.len(), 0);
        assert_eq!(in_schema, out_schema);
    }

    #[test]
    fn test_serialize_schema_random() {
        let num_samples = 100;
        let mut rng = rand::rng();
        for _ in 0..num_samples {
            let in_schema = make_random_schema(&mut rng);
            let bytes = serialize_schema(&in_schema);
            let (out_schema, left_over_bytes) = deserialize_schema(&bytes);
            assert_eq!(left_over_bytes.len(), 0);
            assert_eq!(in_schema, out_schema);
        }
    }
}

// === Part 1.3 Row Serialization ===

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum DataValue {
    Bool(bool),
    Int(u64),
    // Rust's built in f32 floats don't support equality and comparison checks.
    // So we have to use this crate.
    Float(OrderedFloat<f32>),
    String(String),
    Blob(Vec<u8>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Row {
    cols: Vec<Option<DataValue>>,
}

// IMPLEMENT_ME
pub fn serialize_row(row: &Row, schema: &Schema) -> Vec<u8> {
    panic!("nyi");
}

// IMPLEMENT_ME
pub fn deserialize_row(bytes: &[u8], schema: &Schema) -> Row {
    panic!("nyi");
}

// HELPFUL this could be helpful but not required
fn serialize_f32(f: f32) -> [u8; size_of::<f32>()] {
    panic!("nyi");
}

// HELPFUL this could be helpful but not required
fn deserialize_f32(bytes: &[u8]) -> (f32, &[u8]) {
    panic!("nyi");
}

impl std::convert::From<&DataValue> for ColType {

    fn from(value: &DataValue) -> Self {
        match value {
            DataValue::Bool(_) => ColType::Bool,
            DataValue::Int(_) => ColType::Int,
            DataValue::Float(_) => ColType::Float,
            DataValue::String(_) => ColType::String,
            DataValue::Blob(_) => ColType::Blob,
        }
    }
}

// IMPLEMENT_ME
pub fn verify_correct_type(col: &DataValue, schema_col: &SchemaColumn) {
    panic!("nyi");
}

// IMPLEMENT_ME
pub fn serialize_value(value: &DataValue, bytes: &mut Vec<u8>) {
    panic!("nyi");
}

// IMPLEMENT_ME
pub fn deserialize_value<'a>(bytes: &'a [u8], col_type: &ColType) -> (DataValue, &'a [u8]) {
    panic!("nyi");
}

#[cfg(test)]
mod part_1_3_row_serialization_tests {
    // allows us to import all of the cool stuff we built above
    use super::*;

    #[test]
    fn test_serialize_row() {
        let schema = Schema::new(
            vec![
                SchemaColumn {
                    name: "key".to_owned(),
                    col_kind: ColKind::PrimaryKey,
                    col_type: ColType::Int,
                },
                SchemaColumn {
                    name: "value".to_owned(),
                    col_kind: ColKind::Nullable,
                    col_type: ColType::String,
                },
                SchemaColumn {
                    name: "indexed_col".to_owned(),
                    col_kind: ColKind::UniqueIndex,
                    col_type: ColType::Float,
                },
                SchemaColumn {
                    name: "foo".to_owned(),
                    col_kind: ColKind::Normal,
                    col_type: ColType::Blob,
                },
                SchemaColumn {
                    name: "bar".to_owned(),
                    col_kind: ColKind::NonUniqueIndex,
                    col_type: ColType::Bool,
                },
            ],
            "my_table".to_owned(),
        );

        let in_row = Row {
            cols: vec![
                Some(DataValue::Int(0)),
                None,
                Some(DataValue::Float(OrderedFloat(0.5))),
                Some(DataValue::Blob("hello!".to_owned().as_bytes().to_vec())),
                Some(DataValue::Bool(false)),
            ],
        };
        let bytes = serialize_row(&in_row, &schema);
        let out_row = deserialize_row(&bytes, &schema);

        assert_eq!(in_row, out_row);
    }

    #[test]
    fn test_serialize_row_fuzzing() {
        let mut rng: ThreadRng = rand::rng();

        let (rows, schema) = generate_test_rows(1000, &mut rng);
        for in_row in rows {
            let bytes = serialize_row(&in_row, &schema);
            let out_row = deserialize_row(&bytes, &schema);
            assert_eq!(in_row, out_row);
        }
    }
}

// === Part 1.4 test utilities ===

const MAX_RANDOM_DATA_LENGTH: usize = 20;

pub fn generate_random_value(col_type: &ColType, rng: &mut ThreadRng) -> DataValue {

    match col_type {
        ColType::Bool => DataValue::Bool(rng.random::<bool>()),
        ColType::Int => DataValue::Int(rng.random::<u64>()),
        ColType::Float => DataValue::Float(OrderedFloat(rng.random::<f32>())),
        ColType::String => {
            let string = random_string(rng);
            return DataValue::String(string);
        }
        ColType::Blob => {
            let len: usize = rng.random_range(0..=MAX_RANDOM_DATA_LENGTH);

            let bytes: Vec<u8> = (0..len).map(|_| rng.random::<u8>()).collect();
            return DataValue::Blob(bytes);
        }
    }
}

fn generate_random_rows(schema: &Schema, len: usize, rng: &mut ThreadRng) -> Vec<Row> {
    const MAX_ITER: usize = 1000;

    let mut rows = vec![];

    let mut seen: HashMap<&String, HashSet<Option<DataValue>>> = HashMap::new();

    for _ in 0..len {
        let mut cols: Vec<Option<DataValue>> = vec![];

        for c in &schema.columns {
            if let ColKind::Nullable = c.col_kind
                && rng.random::<bool>()
            {
                cols.push(None);
            } else if let ColKind::UniqueIndex | ColKind::PrimaryKey = c.col_kind {
                let mut value = generate_random_value(&c.col_type, rng);
                let mut i = 0;

                while !seen
                    .entry(&c.name)
                    .or_insert_with(|| HashSet::new())
                    .insert(Some(value.clone()))
                {
                    value = generate_random_value(&c.col_type, rng);

                    if i > MAX_ITER {
                        panic!(
                            "couldn't find a unique value for col {} of type {:?} and kind {:?} in {} iterations",
                            c.name, c.col_type, c.col_kind, MAX_ITER
                        );
                    }
                    i += 1;
                }

                cols.push(Some(value));
            } else {
                cols.push(Some(generate_random_value(&c.col_type, rng)));
            }
        }

        rows.push(Row { cols });
    }

    return rows;
}

pub fn make_test_schema(name: String) -> Schema {
    return Schema::new(
        vec![
            SchemaColumn {
                name: "is_alive".to_owned(),
                col_kind: ColKind::Normal,
                col_type: ColType::Bool,
            },
            SchemaColumn {
                name: "address".to_owned(),
                col_kind: ColKind::Nullable,
                col_type: ColType::String,
            },
            SchemaColumn {
                name: "id".to_owned(),
                col_kind: ColKind::PrimaryKey,
                col_type: ColType::Int,
            },
            SchemaColumn {
                name: "coolness".to_owned(),
                col_kind: ColKind::Normal,
                col_type: ColType::Float,
            },
            SchemaColumn {
                name: "name".to_owned(),
                col_kind: ColKind::Normal,
                col_type: ColType::String,
            },
            SchemaColumn {
                name: "json_field".to_owned(),
                col_kind: ColKind::Normal,
                col_type: ColType::Blob,
            },
        ],
        name,
    );
}

pub fn generate_test_rows(len: usize, rng: &mut ThreadRng) -> (Vec<Row>, Schema) {
    let schema = make_test_schema("test_table".to_owned());
    return (generate_random_rows(&schema, len, rng), schema);
}

// === Part 1.4 Alignment Utilities === 

fn filesystem_block_size() -> usize {
    let c_path = CString::new(".").unwrap();
    let mut stat: statvfs = unsafe { std::mem::zeroed() };

    let ret = unsafe { statvfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        panic!("statvfs failed");
    }

    // this is the preferred block size by the file system which can be much bigger
    // on my Mac it is 1MB!
    stat.f_bsize as usize; 

    // minimum block size 
    // https://www.unix.com/man_page/osx/3/statvfs/
    let f_frsize = stat.f_frsize as usize;
    
    return f_frsize;
}

fn assert_correct_page_size() {
    // This is to make sure our pages are aligned to the correct block size for the file system 
    // or else file operations with the O_DIRECT flag will fail with EINVAL.

    // https://man7.org/linux/man-pages/man2/open.2.html NOTES -> O_DIRECT
    // "The O_DIRECT flag may impose alignment restrictions on the length
    //  and address of user-space buffers and the file offset of I/Os."
    // "In Linux 2.4, most filesystems based on block devices require that the file
    // offset and the length and memory address of all I/O segments be
    // multiples of the filesystem block size (typically 4096 bytes)"

    // Technically our database page size and page memory address just needs to be a multiple
    // of the file system block size to avoid that error and could be different. I'm just going to keep it simple and 
    // make them all the same. 
    // The largest number that our memory address is a multiple of is called the "alignment".

    // We also need to make sure our database page size can be written atomically by our hardware page or else  "the DBMS will have to take extra
    // measures to ensure that the data gets written out safely since the program can get partway through writing
    // a database page to disk when the system crashes." https://15445.courses.cs.cmu.edu/fall2025/notes/03-storage1.pdf Page 3 - 7 Database Pages
    // The Rust Playground does not expose the hardware block size as far as I can tell
    // but I'm going to assume it is <= the OS page size. 

    // At the time I wrote this, the block size in the Rust Playground was 4096 bytes.
    let file_system_block_size = filesystem_block_size();
    assert_eq!(PAGE_SIZE, file_system_block_size);
}
#[cfg(test)]
mod page_size_test {
    use super::*;

    #[test]
    fn test_page_size() {
        assert_correct_page_size();
    }
}
#[derive(Debug)]

// If our pointer points into the buffer pool, we don't want to free the memory on drop.
// If our pointer points into a SingleAlignedBuf, we keep the SingleAlignedBuf in the struct.
// That way the memory SingleAlignedBuf owns is not freed until PtrWrapper is dropped, which also drops the SingleAlignedBuf.
pub enum PtrWrapper {
    // pointer into buffer pool
    BufferPool {
        ptr: *mut u8,
    },

    // owned buffer 
    // used by WAL and tests
    Owned {
        buf: SingleAlignedBuf,
    },
}

unsafe impl Send for PtrWrapper {}
// the buffer pool manager will enforce via a RwLock that two threads cannot mutate the same data
// and data cannot be mutated while another thread has an immutable reference to it
unsafe impl Sync for PtrWrapper {}

// we wrap the raw pointer in two methods that are easier to use
// so the Page code can treat it like a slice of bytes
impl PtrWrapper {

    // Most of the pages should use memory in the buffer pool.
    // Only the WAL and tests should use this.
    pub fn new_owned() -> Self {
        let aligned_buf = SingleAlignedBuf::new();
        return Self::Owned{ buf: aligned_buf };
    }

    pub fn from_buffer_pool(ptr: *mut u8) -> Self {
        return Self::BufferPool { ptr };
    }

    pub fn as_slice(&self) -> &[u8] {
        let ptr = match self {
            Self::BufferPool { ptr } => *ptr,
            Self::Owned { buf } => buf.ptr,
        };

        unsafe {
            slice::from_raw_parts(ptr, PAGE_SIZE)
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = match self {
            Self::BufferPool { ptr } => *ptr,
            Self::Owned { buf } => buf.ptr,
        };

        unsafe {
            slice::from_raw_parts_mut(ptr, PAGE_SIZE)
        }
    }
}

pub fn is_aligned(ptr: *mut u8) -> bool {
    return (ptr as usize) % PAGE_SIZE == 0;
}

#[derive(Debug)]
pub struct SingleAlignedBuf {
    ptr: *mut u8,
    len: usize,
    layout: Layout,
}

unsafe impl Send for SingleAlignedBuf {}
unsafe impl Sync for SingleAlignedBuf {}

// this is just for testing
// in our real implementation, we will create our Page from our BufferPool
impl SingleAlignedBuf {
    pub fn new() -> Self {

        // see comment on assert_correct_page_size
        let layout = Layout::from_size_align(PAGE_SIZE, PAGE_SIZE).unwrap();

        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            panic!("Allocation failed");
        }
        
        if !is_aligned(ptr) {
            panic!("not aligned");
        }

        Self {
            ptr,
            len: PAGE_SIZE,
            layout,
        }
    }
}

impl Drop for SingleAlignedBuf {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

// === Part 1.4 Pages ===
pub const PAGE_SIZE: usize = 4 * 1024;
const HEADER_SIZE: usize = size_of::<Header>();
const SLOT_SIZE: usize = size_of::<Slot>();

// I'll explain where the 2^15 - 1 comes from in Part 1.5
const MAX_TUPLE_SIZE: usize = if PAGE_SIZE < 2usize.pow(15) - 1 {
    PAGE_SIZE
} else {
    2usize.pow(15) - 1
};

// Checked at compile time
const _: () = {
    assert!(MAX_TUPLE_SIZE <= PAGE_SIZE);
};

trait BufferTrait: std::fmt::Debug {
    fn as_slice(&self) -> &[u8];

    fn as_mut_slice(&mut self) -> &mut [u8];
}

pub trait PageTrait {
    fn get_tuple(&self, slot_number: usize) -> Vec<u8>;
    fn has_space(&self, row_size: usize) -> bool;
    fn get_data(&self) -> &[u8];
    fn get_all_tuples(&self) -> Vec<(usize, Vec<u8>)>; // (slot_number, tuple)
}

pub trait PageMutTrait {
    fn initialize(&mut self);
    fn insert_tuple(&mut self, tuple: Vec<u8>) -> usize;
    fn update_tuple(&mut self, slot_number: usize, tuple: Vec<u8>);
    fn delete_tuple(&mut self, slot_number: usize);
}

#[derive(Debug)]
pub struct Page {
    data: PtrWrapper,
}

#[derive(Debug)]
struct Header {
    num_tuples: u16,
    left_end_of_tuples: u16,
}

#[derive(Debug)]
struct Slot {
    offset: u16,
    len: u16,
}

impl Page {

    // this tells the compiler to only include this 
    // in tests
    // in our real system, we will use memory allocated in the BufferPool
    pub fn new_owned() -> Self {
        let mut page = Page { data: PtrWrapper::new_owned() };
        let header = Header {
            num_tuples: 0,
            left_end_of_tuples: PAGE_SIZE as u16,
        };
        page.set_header(header);
        return page;
    }

    // this does any setup needed on the data
    // this does not assume the underlying bytes are set to anything
    pub fn new(data: PtrWrapper) -> Self {
        let mut page = Page {
            data,
        };
        let header = Header {
            num_tuples: 0,
            left_end_of_tuples: PAGE_SIZE as u16,
        };
        page.set_header(header);
    
        return page;
    }

    // this just takes the data and makes a page. this assumes
    // the underlying data is a valid page. e.g. it was just read from 
    // disk. 
    pub fn from(data: PtrWrapper) -> Self {
        let page = Page {
            data,
        };

        return page;
    }

    // HELPFUL
    fn set_header(&mut self, new_header: Header) {
        panic!("nyi");
    }

}

impl Page {
    // we'll talk about this more later 
    fn is_deleted(&self, slot_number: usize) -> bool {
        let slot = self.get_slot(slot_number);
        let top_bit_mask: u16 = 1 << 15;
        return slot.len & top_bit_mask != 0;
    }
}

impl PageTrait for Page {
    
    // IMPLEMENT_ME
    fn get_tuple(&self, slot_number: usize) -> Vec<u8> {
        panic!("nyi");
    }

    // IMPLEMENT_ME
    fn has_space(&self, row_size: usize) -> bool {
        panic!("nyi");
    }

    // IMPLEMENT_ME
    fn get_data(&self) -> &[u8] {
        panic!("nyi");
    }

    // we'll implement this in the next part
    fn get_all_tuples(&self) -> Vec<(usize, Vec<u8>)> {
        return self.get_all_tuples_helper();
    }
}

impl PageMutTrait for Page {
    
    fn initialize(&mut self) {
        let header = Header {
            num_tuples: 0,
            left_end_of_tuples: PAGE_SIZE as u16,
        };
        self.set_header(header);
    }

    // should return the slot number of the newly inserted tuple
    // IMPLEMENT_ME
    fn insert_tuple(&mut self, tuple: Vec<u8>) -> usize {
        panic!("nyi");
    }

    // we'll implement this in the next part
    fn delete_tuple(&mut self, slot_number: usize) {
        return self.delete_tuple_helper(slot_number);
    }

    // we'll implement this in the next part
    fn update_tuple(&mut self, slot_number: usize, tuple: Vec<u8>) {
        return self.update_tuple_helper(slot_number, tuple);
    }
}

#[cfg(test)]
mod part_1_4_page_tests {
    // allows us to import all of the cool stuff we built above
    use super::*;

    #[test]
    fn test_page_insert() {
        let num_writes = 10000;

        let mut rng = rand::rng();
        let (input_rows, schema) = generate_test_rows(num_writes, &mut rng);
        let primary_key_index: usize = schema
            .columns
            .iter()
            .position(|c| matches!(c.col_kind, ColKind::PrimaryKey))
            .unwrap();

        let mut pages: Vec<Page> = vec![];
        let mut current_page = Page::new_owned();

        // index mapping from value of primary key column to the (page_number, slot_number) tuple
        let mut index: HashMap<Option<DataValue>, (usize, usize)> = HashMap::new();

        // insert the rows to the pages
        for in_row in input_rows.clone() {
            let bytes = serialize_row(&in_row, &schema);

            // if the current page doesn't have enough space, we will write to a new page
            if !current_page.has_space(bytes.len()) {
                pages.push(current_page);
                current_page = Page::new_owned();
            }

            assert!(current_page.has_space(bytes.len()));

            let slot_number = current_page.insert_tuple(bytes);

            // our index maps from primary key value to (page_number, slot_number)
            index.insert(
                in_row.cols[primary_key_index].clone(),
                (pages.len(), slot_number),
            );
        }

        // add the last page
        pages.push(current_page);

        // check that the rows in the pages are the same as the rows we inserted
        assert_eq!(input_rows.len(), index.len());
        for in_row in &input_rows {
            let pk = &in_row.cols[primary_key_index];
            let (page_number, slot_number) = index.get(pk).unwrap();
            let bytes = pages[*page_number].get_tuple(*slot_number);
            let out_row = deserialize_row(&bytes, &schema);

            assert_eq!(&out_row, in_row);
        }
    }
}

// === Part 1.5 Pages Delete and Update ===

impl Page {
    // IMPLEMENT_ME
    fn update_tuple_helper(&mut self, slot_number: usize, tuple: Vec<u8>) {
        panic!("nyi");
    }

    // IMPLEMENT_ME
    fn delete_tuple_helper(&mut self, slot_number: usize) {
        panic!("nyi");
    }

    // IMPLEMENT_ME
    fn reclaim_space(&mut self) {
        panic!("nyi");
    }
}

impl Page {
    // this should return the tuples in order by slot number
    // and not included any deleted tuples
    // IMPLEMENT_ME
    pub fn get_all_tuples_helper(&self) -> Vec<(usize, Vec<u8>)> {
        panic!("nyi");
    }
}

// test infrastructure

const MAX_RANDOM_TUPLE_LENGTH: usize = 50;

pub fn random_tuple(rng: &mut ThreadRng) -> Vec<u8> {
    let len: usize = rng.random_range(0..=MAX_RANDOM_TUPLE_LENGTH);
    return (0..len).map(|_| rng.random::<u8>()).collect();
}

pub fn random_tuple_min_size(min_size: usize, rng: &mut ThreadRng) -> Vec<u8> {
    let len: usize = rng.random_range(min_size..=MAX_RANDOM_TUPLE_LENGTH);
    return (0..len).map(|_| rng.random::<u8>()).collect();
}

#[cfg(test)]
mod part_1_5_page_tests {
    use rand::seq::IteratorRandom as _;

    // allows us to import all of the cool stuff we built above
    use super::*;

    #[test]
    fn test_page_insert() {
        let num_writes = 10000;

        let mut rng = rand::rng();
        let (input_rows, schema) = generate_test_rows(num_writes, &mut rng);
        let primary_key_index: usize = schema
            .columns
            .iter()
            .position(|c| matches!(c.col_kind, ColKind::PrimaryKey))
            .unwrap();

        let mut pages: Vec<Page> = vec![];
        let mut current_page = Page::new_owned();

        // index mapping from value of primary key column to the (page_number, slot_number) tuple
        let mut index: HashMap<Option<DataValue>, (usize, usize)> = HashMap::new();

        // insert the rows to the pages
        for in_row in input_rows.clone() {
            let bytes = serialize_row(&in_row, &schema);

            // if the current page doesn't have enough space, we will write to a new page
            if !current_page.has_space(bytes.len()) {
                pages.push(current_page);
                current_page = Page::new_owned();
            }

            assert!(current_page.has_space(bytes.len()));

            let slot_number = current_page.insert_tuple(bytes);

            // our index maps from primary key value to (page_number, slot_number)
            index.insert(
                in_row.cols[primary_key_index].clone(),
                (pages.len(), slot_number),
            );
        }

        // add the last page
        pages.push(current_page);

        // check that the rows in the pages are the same as the rows we inserted
        assert_eq!(input_rows.len(), index.len());
        for in_row in &input_rows {
            let pk = &in_row.cols[primary_key_index];
            let (page_number, slot_number) = index.get(pk).unwrap();
            let bytes = pages[*page_number].get_tuple(*slot_number);
            let out_row = deserialize_row(&bytes, &schema);

            assert_eq!(&out_row, in_row);
        }

        // now we will randomly do inserts, deletes and updates

        let mut input_rows: HashMap<Option<DataValue>, Row> = input_rows
            .clone()
            .into_iter()
            .map(|r| (r.cols[primary_key_index].clone(), r))
            .collect();

        for _ in 0..num_writes {
            match rng.random_range(0..=2) {
                0 => {
                    // test UPDATE
                    // choose a random row to update
                    let (pk_value, row) = input_rows.iter_mut().choose(&mut rng).unwrap();
                    // change the row to a random new value
                    let mut new_cols = generate_random_rows(&schema, 1, &mut rng)[0].clone().cols;
                    // but the pk must stay the same
                    new_cols[primary_key_index] = pk_value.clone();
                    // update the row in the map
                    row.cols = new_cols;

                    // get the page where the row is stored
                    let (page_number, slot_number) = index.get(pk_value).unwrap();
                    let page = &mut pages[*page_number];
                    let slot_number = slot_number.clone();

                    let new_row_bytes = serialize_row(row, &schema);

                    // if the page has space then update it there
                    if page.has_space(new_row_bytes.len()) {
                        page.update_tuple(slot_number, new_row_bytes);
                    } else {
                        // if the existing page does not have space then put it in a new page

                        // remove tuple from existing page
                        page.delete_tuple(slot_number);

                        // if the last page doesn't have space, make a brand new page
                        // Note that this is just a test but in a real DBMS you might look for a page with free space
                        // in a more clever way.
                        if !pages[pages.len() - 1].has_space(new_row_bytes.len()) {
                            pages.push(Page::new_owned());
                        }

                        let i = pages.len() - 1;
                        let new_slot_number = pages[i].insert_tuple(new_row_bytes);
                        // update the index to point to the new page which will always be the last page in pages
                        index.insert(pk_value.clone(), (i, new_slot_number));
                    }
                }
                1 => {
                    // test INSERT
                    let new_row = generate_random_rows(&schema, 1, &mut rng)[0].clone();
                    let new_row_bytes = serialize_row(&new_row, &schema);
                    if !index.contains_key(&new_row.cols[primary_key_index]) {
                        // if the last page doesn't have space, make a brand new page
                        // Note that this is just a test but in a real DBMS you might look for a page with free space
                        // in a more clever way.
                        if !pages[pages.len() - 1].has_space(new_row_bytes.len()) {
                            pages.push(Page::new_owned());
                        }

                        let i = pages.len() - 1;
                        let slot_number = pages[i].insert_tuple(new_row_bytes);
                        index.insert(new_row.cols[primary_key_index].clone(), (i, slot_number));
                        input_rows.insert(new_row.cols[primary_key_index].clone(), new_row);
                    }
                }
                2 => {
                    // test DELETE
                    let to_delete;
                    {
                        let (pk_value, _) = input_rows.iter_mut().choose(&mut rng).unwrap();
                        to_delete = pk_value.clone();

                        let (page_number, slot_number) = index.get(pk_value).unwrap();
                        pages[*page_number].delete_tuple(*slot_number);

                        index.remove(pk_value);
                    }
                    input_rows.remove(&to_delete);
                }
                _ => unreachable!(),
            }
        }

        // test the rows in the pages + index are what we expect
        // this simulates a DB with an index storage architecture
        assert_eq!(input_rows.len(), index.len());
        for (key, in_row) in &input_rows {
            let (page_number, slot_number) = index.get(key).unwrap();
            let bytes = pages[*page_number].get_tuple(*slot_number);
            let out_row = deserialize_row(&bytes, &schema);
            assert_eq!(&out_row, in_row);
        }

        // test the rows in the pages are what we expect without an index
        // this simulates a DB with no index and just a row-oriented storage architecture
        let mut new_index: HashMap<Option<DataValue>, Row> = HashMap::new();
        for p in pages {
            for (_, bytes) in p.get_all_tuples() {
                let row = deserialize_row(&bytes, &schema);
                new_index.insert(row.cols[primary_key_index].clone(), row);
            }
        }
        assert_eq!(input_rows, new_index);
    }

    #[test]
    fn test_reclaim_space() {
        let mut rng = rand::rng();
        let num_trials = 1000;

        for _ in 0..num_trials {
            let mut page = Page::new_owned();
            let mut tuple = random_tuple_min_size(SLOT_SIZE + 1, &mut rng);
            let mut index = HashMap::new();

            while page.has_space(tuple.len()) {
                let slot_no = page.insert_tuple(tuple.clone());
                index.insert(slot_no, tuple);

                tuple = random_tuple_min_size(SLOT_SIZE + 1, &mut rng);
            }

            let (slot_no, tuple) = index.iter().next().unwrap();
            // this is a litle silly but we have convince the compiler it is safe to mutate index 
            let slot_no = *slot_no; 
            let tuple_len = tuple.len();
            index.remove(&slot_no);
            page.delete_tuple(slot_no);

            // we removed the tuple and got back that space but the next tuple has to also add a slot
            let free_space = tuple_len - SLOT_SIZE;
            assert!(free_space > 0);
            let tuple: Vec<u8> = (0..free_space).map(|_| rng.random::<u8>()).collect();
            assert!(page.has_space(tuple.len()));
            page.insert_tuple(tuple);
        }

    }
}

// === Part 1.6 Table Serialization & Formatting ===
#[derive(Debug, PartialEq)]
pub struct Table {
    rows: Vec<Row>,
    schema: Schema,
}

impl Table {
    pub fn new(schema: Schema, rows: Vec<Row>) -> Self {
        return Table{ rows, schema };
    }

    pub fn get_schema(&self) -> Schema {
        return self.schema.clone();
    }
}

// IMPLEMENT_ME
pub fn serialize_table(table: &Table, create_page: fn() -> Page) -> Vec<Page> {
    panic!("nyi");
}

// IMPLEMENT_ME
pub fn deserialize_table(pages: Vec<Page>) -> Table {
    panic!("nyi");
}

impl fmt::Display for Table {
    // IMPLEMENT_ME
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("nyi");
    }
}

impl Table {
    fn format_row(&self, row_data: &[String], column_widths: &[usize]) -> String {
        row_data
            .iter()
            .enumerate()
            // this adds the appropriate amount of padding
            .map(|(i, value)| format!("{:<width$}", value, width = column_widths[i]))
            .collect::<Vec<String>>()
            .join(" | ")
    }

    // Calculate the maximum width for each column based on the column names and data
    // This makes sure when we print the table, the columns are all lined up
    fn calculate_column_widths(&self) -> Vec<usize> {
        self.schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let col_name_width = col.name.len();
                let max_data_width = self
                    .rows
                    .iter()
                    .map(|row| match &row.cols[i] {
                        Some(value) => format!("{:?}", value).len(),
                        None => "NULL".len(),
                    })
                    .max()
                    .unwrap_or(0);

                // The width is the max of the column name width and the max data width
                return max(col_name_width, max_data_width);
            })
            .collect()
    }
}

#[cfg(test)]
mod part_1_6_table_tests {
    // allows us to import all of the cool stuff we built above
    use super::*;

    #[test]
    fn test_serde_table() {
        let mut rng = rand::rng();
        let (rows, schema) = generate_test_rows(1000, &mut rng);

        let in_table = Table { rows, schema };
        let mut pages = serialize_table(&in_table, Page::new_owned);

        let out_table = deserialize_table(pages);

        assert_eq!(out_table.schema, in_table.schema);

        let primary_key_index: usize = in_table
            .schema
            .columns
            .iter()
            .position(|c| matches!(c.col_kind, ColKind::PrimaryKey))
            .unwrap();

        let mut in_table_map: HashMap<Option<DataValue>, Row> = HashMap::new();
        for row in in_table.rows {
            in_table_map.insert(row.cols[primary_key_index].clone(), row);
        }

        assert_eq!(in_table_map.len(), out_table.rows.len());

        for out_row in out_table.rows {
            let in_row = in_table_map.get(&out_row.cols[primary_key_index]).unwrap();
            assert_eq!(in_row, &out_row);
        }
    }

    #[test]
    fn print_table() {
        let mut rng = rand::rng();
        let (rows, schema) = generate_test_rows(20, &mut rng);

        let table = Table { rows, schema };
        println!("{}", table);
    }
}

// === Part 1.Optional Boilerplate ===

// We need to implement an ordering of DataValue so we can sort it.
// Hashmaps that can take any type are surprisingly difficult in Rust and the crates that handle them
// are not in the Rust Playground so we need this little Ord trick and a BTreeMap.
impl Ord for DataValue {
    fn cmp(&self, other: &Self) -> Ordering {
        use DataValue::*;

        match (self, other) {
            (Bool(a), Bool(b)) => a.cmp(b),
            (Int(a), Int(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.cmp(b),
            (String(a), String(b)) => a.cmp(b),
            (Blob(a), Blob(b)) => a.cmp(b),

            // In the case when the types are not the same we use an arbitrary ordering of types.
            // We are only expecting to compare values within a column so this should not have any real effect
            // and is only here to satisfy the Ord trait so we can use DataValue in a BTreemap.
            (a, b) => variant_tag(a).cmp(&variant_tag(b)),
        }
    }
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn variant_tag(value: &DataValue) -> u8 {
    match value {
        DataValue::Bool(_) => 0,
        DataValue::Int(_) => 1,
        DataValue::Float(_) => 2,
        DataValue::String(_) => 3,
        DataValue::Blob(_) => 4,
    }
}

// === Part 1.Optional Columnar Storage & Compression ===

// IMPLEMENT_ME
pub fn get_columnar_from_rows(rows: Vec<Row>, schema: &Schema) -> Vec<u8> {
    panic!("nyi");
}

// IMPLEMENT_ME
pub fn get_rows_from_columnar(bytes: &[u8], schema: &Schema) -> Vec<Row> {
    panic!("nyi");
}

#[cfg(test)]
mod tests {
    // allows us to import all of the cool stuff we built above
    use super::*;

    #[test]
    fn part_1_optional_tests() {
        let num_test_rows = 10000;
        let mut rng = rand::rng();
        let (input_rows, schema) = generate_test_rows(num_test_rows, &mut rng);

        let mut row_store_len = 0;
        for row in input_rows.clone() {
            row_store_len += serialize_row(&row, &schema).len();
        }

        let bytes = get_columnar_from_rows(input_rows.clone(), &schema);
        let output_rows = get_rows_from_columnar(&bytes, &schema);

        println!(
            "compression ratio: {} / {}, {:?}%",
            bytes.len(),
            row_store_len,
            bytes.len() / row_store_len
        );

        let primary_key_index: usize = schema
            .columns
            .iter()
            .position(|c| matches!(c.col_kind, ColKind::PrimaryKey))
            .unwrap();
        let output_map: HashMap<Option<DataValue>, &Row> = output_rows
            .iter()
            .map(|x| (x.cols[primary_key_index].clone(), x))
            .collect();
        let input_map: HashMap<Option<DataValue>, &Row> = input_rows
            .iter()
            .map(|x| (x.cols[primary_key_index].clone(), x))
            .collect();

        assert_eq!(output_map, input_map);
    }
}

// === Part 1.Optional Helpful Stuff

// You don't have to use any of this. Feel free to delete or modify it however you like

// map from value to set of row indexes that contain the value e.g.
//      a | b
//      3, 'cat'
//      11, 'dog'
//      3, 'rat'
// for a { 3: { 0, 2 }, 11: { 1 } }
// for b { 'cat': { 0 }, 'dog': { 1 }, 'rat': { 2 } }
type ColumnStats = BTreeMap<Option<DataValue>, BTreeSet<usize>>;

#[derive(Debug)]
struct RLEValue {
    value: Option<DataValue>,
    length: usize,
}
