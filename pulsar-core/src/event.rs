use std::{fmt, net::IpAddr, time::SystemTime};

use serde::{Deserialize, Serialize};
use validatron::{
    Operator, Primitive, ValidatronError, ValidatronStruct, ValidatronTypeProvider,
    ValidatronVariant,
};

use crate::{
    kernel::{self},
    pdk::ModuleName,
};

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct Event {
    pub header: Header,
    pub payload: Payload,
}

impl ValidatronVariant for Event {
    fn validate(
        variant: &str,
        field_compare: &validatron::Field,
        op: validatron::Operator,
        value: &str,
    ) -> Result<(usize, Box<dyn Fn(&Self) -> bool + Send + Sync>), validatron::ValidatronError>
    {
        match field_compare {
            validatron::Field::Simple(s) => {
                Err(validatron::ValidatronError::FieldNotSimple(s.to_string()))
            }
            validatron::Field::Struct { name, inner_field } => match name.as_str() {
                "header" => {
                    let var_num = Payload::var_num_of(variant)?;
                    let validated_struct = validatron::process_struct(
                        inner_field,
                        |event: &Self| &event.header,
                        op,
                        value,
                    );
                    validated_struct.map(|vc| (var_num, vc))
                }

                "payload" => validatron::process_variant(
                    variant,
                    inner_field,
                    |event: &Self| &event.payload,
                    op,
                    value,
                ),
                _ => Err(validatron::ValidatronError::FieldNotFound(name.clone())),
            },
        }
    }

    fn var_num(&self) -> usize {
        self.payload.var_num()
    }

    fn var_num_of(variant: &str) -> Result<usize, validatron::ValidatronError> {
        Payload::var_num_of(variant)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ValidatronStruct)]
pub struct Header {
    pub image: String,
    pub pid: i32,
    pub parent_pid: i32,
    pub is_threat: bool,
    pub source: ModuleName,
    #[validatron(skip)]
    pub timestamp: SystemTime,
    #[validatron(skip)]
    pub fork_time: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, ValidatronVariant)]
#[serde(tag = "type", content = "content")]
pub enum Payload {
    FileCreated {
        filename: String,
    },
    FileDeleted {
        filename: String,
    },
    DirCreated {
        dirname: String,
    },
    DirDeleted {
        dirname: String,
    },
    FileOpened {
        filename: String,
        flags: FileFlags,
    },
    FileLink {
        source: String,
        destination: String,
        hard_link: bool,
    },
    FileRename {
        source: String,
        destination: String,
    },
    ElfOpened {
        filename: String,
        flags: FileFlags,
    },
    Fork {
        ppid: i32,
    },
    Exec {
        filename: String,
        argc: usize,
        argv: Argv,
    },
    Exit {
        exit_code: u32,
    },
    SyscallActivity {
        #[validatron(skip)]
        histogram: Vec<u64>,
    },
    Bind {
        address: Host,
        is_tcp: bool,
    },
    Listen {
        address: Host,
    },
    Connect {
        destination: Host,
        is_tcp: bool,
    },
    Accept {
        source: Host,
        destination: Host,
    },
    Close {
        source: Host,
        destination: Host,
    },
    Receive {
        source: Host,
        destination: Host,
        len: usize,
        is_tcp: bool,
    },
    DnsQuery {
        #[validatron(skip)]
        questions: Vec<DnsQuestion>,
    },
    DnsResponse {
        #[validatron(skip)]
        questions: Vec<DnsQuestion>,
        #[validatron(skip)]
        answers: Vec<DnsAnswer>,
    },
    Send {
        source: Host,
        destination: Host,
        len: usize,
        is_tcp: bool,
    },
    MalwareDetection {
        score: f32,
        #[validatron(skip)]
        tags: Vec<String>,
    },
    RuleEngineDetection {
        #[validatron(skip)]
        rule_name: String,
        #[validatron(skip)]
        payload: Box<Payload>,
    },
    AnomalyDetection {
        score: f32,
    },
    // CustomJson { ty: i32, data: Vec<u8> },
    // CustomProto { ty: i32, data: Vec<u8> },
    // CustomRaw { ty: i32, data: Vec<u8> }
}

/// Encapsulates IP and port.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    ValidatronStruct,
    ValidatronTypeProvider,
)]
pub struct Host {
    pub ip: IpAddr,
    pub port: u16,
}

/// Encapsulates data of a DNS question.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsQuestion {
    /// Question name string.
    pub name: String,
    /// Question type.
    pub qtype: String,
    /// Question class.
    pub qclass: String,
}

/// Encapsulates data of a DNS answer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsAnswer {
    /// Name string.
    pub name: String,
    /// Answer record class.
    pub class: String,
    /// Record TTL.
    pub ttl: u32,
    /// Record data.
    pub data: String,
}

// High level abstraction for file flags bitmask
#[repr(C)]
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileFlags(i32);

impl FileFlags {
    pub fn from_raw_unchecked(flags: i32) -> Self {
        Self(flags)
    }
}

impl FileFlags {
    const ACC_MODE_FLAGS: [(&'static str, i32); 3] = [
        ("O_RDONLY", kernel::file::flags::O_RDONLY),
        ("O_WRONLY", kernel::file::flags::O_WRONLY),
        ("O_RDWR", kernel::file::flags::O_RDWR),
    ];

    const OTHER_FLAGS: [(&'static str, i32); 7] = [
        ("O_CREAT", kernel::file::flags::O_CREAT),
        ("O_EXCL", kernel::file::flags::O_EXCL),
        ("O_NOCTTY", kernel::file::flags::O_NOCTTY),
        ("O_TRUNC", kernel::file::flags::O_TRUNC),
        ("O_APPEND", kernel::file::flags::O_APPEND),
        ("O_NONBLOCK", kernel::file::flags::O_NONBLOCK),
        ("O_DIRECTORY", kernel::file::flags::O_DIRECTORY),
    ];
}

impl ValidatronTypeProvider for FileFlags {
    fn field_type() -> validatron::ValidatronType<Self> {
        validatron::ValidatronType::Primitive(Primitive {
            parse_fn: Box::new(|s| {
                FileFlags::ACC_MODE_FLAGS
                    .iter()
                    .chain(FileFlags::OTHER_FLAGS.iter())
                    .find(|(name, _)| *name == s)
                    .map(|(_, flag)| Self(*flag))
                    .ok_or_else(|| ValidatronError::FieldValueParseError(s.to_string()))
            }),
            handle_op_fn: Box::new(|op| match op {
                Operator::Multi(op) => match op {
                    validatron::MultiOperator::Contains => Ok(Box::new(|a, b| {
                        if FileFlags::ACC_MODE_FLAGS
                            .iter()
                            .any(|(_, acc_mode_flag)| acc_mode_flag == &b.0)
                        {
                            let mode = a.0 & kernel::file::flags::O_ACCMODE;
                            mode == b.0
                        } else {
                            (a.0 & b.0) > 0
                        }
                    })),
                },
                _ => Err(ValidatronError::OperatorNotAllowedOnType(
                    op,
                    "FileFlags".to_string(),
                )),
            }),
        })
    }
}

impl fmt::Debug for FileFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.0, self)
    }
}

impl fmt::Display for FileFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut flag_names = Vec::new();

        let mode = self.0 & kernel::file::flags::O_ACCMODE;
        for (name, flag) in FileFlags::ACC_MODE_FLAGS {
            if mode == flag {
                flag_names.push(name);
                break; // Only one is possible
            }
        }

        for (name, flag) in FileFlags::OTHER_FLAGS {
            if (self.0 & flag) > 0 {
                flag_names.push(name);
            }
        }

        let content = flag_names.join(",");

        write!(f, "({content})")
    }
}

impl From<FileFlags> for i32 {
    fn from(f_flags: FileFlags) -> Self {
        f_flags.0
    }
}

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Argv(Vec<String>);

impl From<Vec<String>> for Argv {
    fn from(argv_list: Vec<String>) -> Self {
        Self(argv_list)
    }
}

impl ValidatronTypeProvider for Argv {
    fn field_type() -> validatron::ValidatronType<Self> {
        validatron::ValidatronType::Primitive(Primitive {
            parse_fn: Box::new(|s| Ok(Argv(vec![s.to_string()]))),
            handle_op_fn: Box::new(|op| match op {
                Operator::Multi(op) => match op {
                    validatron::MultiOperator::Contains => {
                        Ok(Box::new(|a, b| b.0.iter().all(|item| a.0.contains(item))))
                    }
                },
                _ => Err(ValidatronError::OperatorNotAllowedOnType(
                    op,
                    "Argv".to_string(),
                )),
            }),
        })
    }
}

impl From<Argv> for Vec<String> {
    fn from(argv: Argv) -> Self {
        argv.0
    }
}
