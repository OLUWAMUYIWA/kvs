use std::{collections::HashMap, ffi::OsStr, fs::{self, File, OpenOptions}, io::{self, Read, Seek, Write}, ops::Index, path::{Path, PathBuf}};
use std::io::{BufReader, BufWriter, SeekFrom, Result as IOResult};
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::hash_map::Entry;

pub struct KvStore {
    path: PathBuf,
    gen: u64,
    readers: HashMap<u64, ReaderPos<File>>,
    writer: WriterPos<File>,
    index: HashMap<String, CmdOffset>,
    uncompacted_num: u64,
}
#[derive(Debug, Serialize, Deserialize)]
enum  Command {
    Set{key: String, val: String},
    Rm{key: String},
}
pub struct CmdOffset {
    gen: u64,
    start: u64,
    end: u64,
}

pub struct ReaderPos<R: Read + Seek> {
    reader: BufReader<R>,
    //we store the posotion of the seek so that we can update it as we please
    pos: u64,
}

impl<R: Read + Seek> ReaderPos<R> {
    fn new(reader: R) -> Result<Self> {
        let mut reader = BufReader::new(reader);
        let pos = reader.seek(SeekFrom::Start(0))?;
        Ok( Self {
            reader,
            pos
        })
    }
}
impl<R: Read + Seek> Read for ReaderPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        let n = self.reader.read(buf)?;
        self.pos += n as u64;
        Ok(self.pos as usize)
    }
}

impl<R: Read + Seek> Seek for ReaderPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

pub struct WriterPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}
impl<W: Write + Seek> WriterPos<W> {
    pub fn  new(w: W) -> Result<Self> {
        let mut writer = BufWriter::new(w);
        let pos = writer.seek(SeekFrom::Start(0))?;
        Ok(Self {
            writer,
            pos,
        })
    }
}
impl<W: Write + Seek> Write for WriterPos<W> {
    fn write(&mut self, buf: &[u8]) -> IOResult<usize> {
        let n = self.writer.write(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> IOResult<()> {
        self.writer.flush()
    }
}
impl<W: Write + Seek> Seek for WriterPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        let pos = self.writer.seek(pos)?;
        self.pos = pos;
        Ok(pos)
    }
}
fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

impl KvStore {
    pub fn new() -> Self {
        todo!()
    }
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let mut readers = HashMap::new();
        let mut index = HashMap::new();
        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;
        for &gen in &gen_list {
            let f = File::open(log_path(&path, gen))?;
            let mut reader = ReaderPos::new(f)?;
            uncompacted += load(gen, &mut index, &mut reader)?;
            readers.insert(gen, reader);
        }
        
        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log(&path, current_gen, &mut readers)?;

        Ok(Self {
            path,
            readers,
            writer,
            gen: current_gen,
            index,
            uncompacted_num: uncompacted,
        })
    }
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::Set{key, val};
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set{key, ..} = cmd {
            self.index.insert(key, CmdOffset{gen: self.gen, start: pos, end: pos + self.writer.pos});
        }
        
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.entry(key) {
            Entry::Occupied(off) => {
                let CmdOffset{gen, start, end} = off.get(); 
                    let rdr = self.readers.get_mut(gen).expect("generation not found");
                    rdr.seek(SeekFrom::Start(start.clone()))?;
                    let take_rdr = rdr.take(end.clone() - start.clone());
                    if let Ok(Command::Set{key: _, val}) = serde_json::from_reader(take_rdr) {
                        Ok(Some(val))
                    } else {
                        Err(anyhow::anyhow!("Command is not in the store"))
                    }
                
            }
            Entry::Vacant(_) => {
                Err(anyhow::anyhow!("key does not exist in the db"))
            }
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.index.remove_entry(&key)
        .ok_or(anyhow::anyhow!("key you're trying to remove does not exist"))
        .and_then(|v| {
            let cmd = Command::Rm{key};
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            Ok(())
        })  
    }
    pub fn compact(&mut self) -> Result<()> {
        let compaction_gen = self.gen + 1;
        self.gen += 2;
        self.writer = new_log(&self.path, self.gen, &mut self.readers)?;
        let mut compact_writer = new_log(&self.path, compaction_gen, &mut self.readers)?;
        let mut new_pos = 0;
        for cmd_off in &mut self.index.values_mut() {
            let reader = self.readers.get_mut(&mut cmd_off.gen).context("unable to find reader for this gen")?;
            if reader.pos != cmd_off.start {
                reader.seek(SeekFrom::Start(cmd_off.start))?;
            }
            let mut curr_reader = reader.take(cmd_off.end - cmd_off.start);
            let n = io::copy(&mut curr_reader, &mut compact_writer)?;
            *cmd_off = CmdOffset{gen: compaction_gen, start: new_pos, end: new_pos + n};
            new_pos += n;
        }
        compact_writer.flush()?;
        let stales = self.readers.keys().filter(|&&gen| gen < compaction_gen).cloned().collect::<Vec<_>>();
        for stale_gen in stales {
            self.readers.remove(&stale_gen);
            fs::remove_file(
                self.path.join(format!("{}.log", stale_gen))
            ).expect("file not found");
        };
        self.uncompacted_num = 0;
        Ok(())
    }
}

fn new_log(path: &Path, gen: u64, readers: &mut HashMap<u64, ReaderPos<File>>) -> Result<WriterPos<File>>{
    let path = path.join(format!("gen-{}.log",gen));
    let rf = File::open(&path)?;
    readers.insert(gen, ReaderPos::new(rf)?);
    let wf =OpenOptions::new().create(true).write(true).append(true).open(&path)?;
    Ok(WriterPos::new(wf)?)
}

fn load(gen: u64, index: &mut HashMap<String, CmdOffset>, reader: &mut ReaderPos<File>) -> Result<u64> {
    let mut start = reader.seek(SeekFrom::Start(0))?;
    let mut uncompacted = 0;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let cmd = cmd?;
        let end = stream.byte_offset() as u64;
        match cmd {
            Command::Set{key, ..} => {
                if let Some(old_cmd) = index.insert(key, CmdOffset{gen, start, end}) {
                    uncompacted += old_cmd.end - old_cmd.start;
                };
            }
            Command::Rm{key} => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.end - old_cmd.start;
                };
                uncompacted += end-start;
            }
        }
        start = end;
    }
    Ok(uncompacted)
}

fn sorted_gen_list(path: impl AsRef<Path>) -> Result<Vec<u64>> {
    let mut gen_list = fs::read_dir(&path)?.flat_map(|res| -> Result<_> {Ok(res?.path())})
    .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
    .flat_map(|path| {
        path.file_name().and_then(OsStr::to_str).map(|s| s.trim_end_matches(".log")).map(str::parse::<u64>)
    }).flatten().collect::<Vec<_>>();
    gen_list.sort_unstable();
    Ok(gen_list)
}