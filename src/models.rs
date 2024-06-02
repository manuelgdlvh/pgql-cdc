use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bytes::{Buf, Bytes};
use lazy_static::lazy_static;
lazy_static! {
    pub static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}


#[non_exhaustive]
#[derive(Debug)]
pub enum ReplicationMessage {
    XLogData(XLogDataBody),
    PrimaryKeepAlive(PrimaryKeepAliveBody),
}


impl TryFrom<Bytes> for ReplicationMessage {
    type Error = ();
    fn try_from(mut buffer: Bytes) -> Result<Self, Self::Error> {
        match buffer.get_u8() {
            b'w' => {
                Ok(ReplicationMessage::XLogData(XLogDataBody::try_from(buffer)?))
            }
            b'k' => {
                let wal_end = buffer.get_u64();
                let ts = buffer.get_i64();
                let timestamp = if ts > 0 {
                    *PG_EPOCH + Duration::from_micros(ts as u64)
                } else {
                    *PG_EPOCH - Duration::from_micros(-ts as u64)
                };
                let reply = buffer.get_u8();
                Ok(ReplicationMessage::PrimaryKeepAlive(PrimaryKeepAliveBody {
                    wal_end,
                    timestamp,
                    reply,
                }))
            }
            _ => { return Err(()); }
        }
    }
}

#[derive(Debug)]
pub struct PrimaryKeepAliveBody {
    pub(crate) wal_end: u64,
    pub(crate) timestamp: SystemTime,
    pub(crate) reply: u8,
}

impl PrimaryKeepAliveBody {
    #[inline]
    pub fn wal_end(&self) -> u64 {
        self.wal_end
    }

    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    #[inline]
    pub fn reply(&self) -> u8 {
        self.reply
    }
}


#[derive(Debug)]
pub struct XLogDataBody {
    pub(crate) wal_start: u64,
    pub(crate) wal_end: u64,
    pub(crate) timestamp: SystemTime,
    pub(crate) data: InnerData,
}

impl TryFrom<Bytes> for XLogDataBody {
    type Error = ();

    fn try_from(mut buffer: Bytes) -> Result<Self, Self::Error> {
        let wal_start = buffer.get_u64();
        let wal_end = buffer.get_u64();
        let ts = buffer.get_i64();
        let timestamp = if ts > 0 {
            *PG_EPOCH + Duration::from_micros(ts as u64)
        } else {
            *PG_EPOCH - Duration::from_micros(-ts as u64)
        };

        Ok(XLogDataBody {
            wal_start,
            wal_end,
            timestamp,
            data: InnerData::try_from(buffer.slice(0..))?,
        })
    }
}

impl XLogDataBody {
    #[inline]
    pub fn wal_start(&self) -> u64 {
        self.wal_start
    }

    #[inline]
    pub fn wal_end(&self) -> u64 {
        self.wal_end
    }

    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    #[inline]
    pub fn data(&self) -> &InnerData {
        &self.data
    }

    #[inline]
    pub fn into_data(self) -> InnerData {
        self.data
    }
}


#[non_exhaustive]
#[derive(Debug)]
pub enum InnerData {
    Insert(InsertBody),
}


impl TryFrom<Bytes> for InnerData {
    type Error = ();

    fn try_from(mut buffer: Bytes) -> Result<Self, Self::Error> {
        match buffer.get_u8() {
            b'I' => {
                let rel_id = buffer.get_u32();
                let tuple = match buffer.get_u8() {
                    b'N' => {
                        Tuple::try_from(buffer)?
                    }
                    _ => {
                        return Err(());
                    }
                };

                Ok(Self::Insert(InsertBody { rel_id, tuple }))
            }
            _ => { return Err(()); }
        }
    }
}

#[derive(Debug)]
pub struct Tuple(Vec<TupleData>);

impl Tuple {
    #[inline]
    pub fn tuple_data(&self) -> &[TupleData] {
        &self.0
    }
}

impl TryFrom<Bytes> for Tuple {
    type Error = ();

    fn try_from(mut buffer: Bytes) -> Result<Self, Self::Error> {
        let col_len = buffer.get_i16();
        let mut tuples = Vec::with_capacity(col_len as usize);

        for _ in 0..col_len {
            let type_tag = buffer.get_u8();
            println!("{}", type_tag);
            let tuple = match type_tag {
                b'n' => TupleData::Null,
                b'u' => TupleData::UnchangedToast,
                b't' => {
                    let len = buffer.get_i32() as usize;
                    let tuple = TupleData::Text(buffer.slice(0..len));
                    buffer.advance(len);
                    tuple
                }
                _ => {
                    return Err(());
                }
            };
            tuples.push(tuple);
        }

        Ok(Tuple(tuples))
    }
}

#[derive(Debug)]
pub enum TupleData {
    Null,
    UnchangedToast,
    Text(Bytes),
}


#[derive(Debug)]
pub struct InsertBody {
    rel_id: u32,
    tuple: Tuple,
}

impl InsertBody {
    #[inline]
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    pub fn tuple(&self) -> &Tuple {
        &self.tuple
    }
}