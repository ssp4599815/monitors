package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"strconv"
)

/*
一个 RDB 文件可以分为以下几个部分：
+-------+-------------+-----------+-----------------+-----+-----------+
| REDIS | RDB-VERSION | SELECT-DB | KEY-VALUE-PAIRS | EOF | CHECK-SUM |
+-------+-------------+-----------+-----------------+-----+-----------+

                      |<-------- DB-DATA ---------->|
*/

// RDB CONSTANTS
const (
	RDB6BitLen  = 0
	RDB14BitLen = 1
	RDB32BitLen = 0x80
	RDB64BitLen = 0x81
	RDBEncVal   = 3
	//  Special RDB opcodes
	RDBOpcodeModuleAux    = 247
	RDBOpcodeIdle         = 248 //LRU idle time.
	RDBOpcodeFreq         = 249 //LFU frequency
	RDBOpcodeAux          = 250 //辅助标识
	RDBOpcodeResizeDB     = 251 //提示调整哈希表大小的操作码
	RDBOpcodeExpireTimeMS = 252 //过期时间毫秒
	RDBOpcodeExpireTime   = 253 //过期时间秒
	RDBOpcodeSelectDB     = 254 //选择数据库的操作
	RDBOpcodeEOF          = 255 //EOF码

	RDBModuleOpcodeEOF    = 0 // End of module value.
	RDBModuleOpcodeSInt   = 1
	RDBModuleOpcodeUInt   = 2
	RDBModuleOpcodeFloat  = 3
	RDBModuleOpcodeDouble = 4
	RDBModuleOpcodeString = 5
)

type RDB struct {
	rd *bufio.Reader

	version int

	expiry uint64 // 当前key的过期时间，毫秒计算
	idle   uint64
	freq   byte
}

func NewRDB(rd *bufio.Reader) *RDB {
	r := &RDB{
		rd: rd,
	}
	return r
}

func (r *RDB) Parse() (err error) {
	// 获取  REDIS | RDB-VERSION
	headbuf := make([]byte, 9)
	_, err = io.ReadFull(r.rd, headbuf)
	if err != nil {
		return
	}
	// 1. verify REDIS SPECIAL string
	// 2. verify VERSION
	err = r.verifyHeader(headbuf)
	if err != nil {
		return err
	}

	log.Infof("Start parse rdb file")

	for {
		// 初始化
		r.expiry = 0
		r.idle = 0
		r.freq = 0

		/* Read type. */
		// 首先读出类型
		dtype, err := r.rd.ReadByte()
		if err != nil {
			return err
		}
		fmt.Println(string(dtype), dtype)
		/* Handle special types. */
		if dtype == RDBOpcodeExpireTime {
			/* EXPIRETIME: load an expire associated with the next key to load */
			var expireSecond uint32
			err = binary.Read(r.rd, binary.LittleEndian, &expireSecond)
			if err != nil {
				return err
			}
			r.expiry = uint64(expireSecond * 1000)
			fmt.Println("过期时间为：", r.expiry)
			continue

		} else if dtype == RDBOpcodeExpireTimeMS { // 新版本 rdb 都使用 RDB_OPCODE_EXPIRETIME_MS
			/* EXPIRETIME_MS: milliseconds precision expire times introduced */
			err := binary.Read(r.rd, binary.LittleEndian, &r.expiry) // 必须为 & 才可以取到值
			if err != nil {
				return err
			}
			fmt.Println("过期时间为：", r.expiry)
			continue
		} else if dtype == RDBOpcodeFreq {
			/* FREQ: LFU frequency. */
			freq, err := r.rd.ReadByte()
			if err != nil {
				return err
			}

			fmt.Println("LFU frequency:", freq)

			continue
		} else if dtype == RDBOpcodeIdle {
			/* IDLE: LRU idle time. */
			idle, err := r.readLength()
			if err != nil {
				return err
			}
			fmt.Println("LRU idle time:", idle)
			continue
		} else if dtype == RDBOpcodeEOF {
			/* EOF: End of file, exit the main loop. */
			log.Infof("finish rdb reading of upstream")
			//  8字节的 CRC64 表示的文件校验和
			// TODO: Verify the checksum if RDB version is >= 5
			if r.version >= 5 {
				_, err := io.ReadFull(r.rd, headbuf[:8])
				if err != nil {
					return nil
				}

				fmt.Println("rdb_checksum", headbuf)
			}
			return nil
		} else if dtype == RDBOpcodeSelectDB {
			/* SELECTDB: Select the specified database. */
			dbnum, err := r.readLength()
			fmt.Println(dbnum)

			if err != nil {
				return err
			}

			continue
		} else if dtype == RDBOpcodeResizeDB {
			/* RESIZEDB: Hint about the size of the keys in the currently selected data base */
			dbSize, err := r.readLength()
			if err != nil {
				return err
			}
			expireSize, err := r.readLength()
			if err != nil {
				return err
			}

			fmt.Println("dbSize, expireSize", dbSize, expireSize)
			continue
		} else if dtype == RDBOpcodeAux {
			var (
				auxKey []byte
				auxVal []byte
			)
			auxKey, err := r.readString()
			if err != nil {
				return err
			}
			auxVal, err = r.readString()
			if err != nil {
				return err
			}
			fmt.Println("auxKey, auxVal", auxKey, auxVal)
			continue
		} else if dtype == RDBOpcodeModuleAux {
			_, err := r.readLength()
			if err != nil {
				return err
			}
			var opcode uint64
			opcode, err = r.readLength()
			for opcode != RDBModuleOpcodeEOF {
				switch opcode {
				case RDBModuleOpcodeSInt, RDBModuleOpcodeUInt:
					_, err = r.readLength()
					if err != nil {
						return err
					}
				case RDBModuleOpcodeFloat:
					_, err = r.readBinaryFloat()
					if err != nil {
						return err
					}
				case RDBModuleOpcodeDouble:
					_, err = r.readBinaryDouble()
					if err != nil {
						return err
					}
				case RDBModuleOpcodeString:
					_, err = r.readString()
					if err != nil {
						return err
					}
				default:
					err = fmt.Errorf("unknown module opcode %d", opcode)
					return err
				}

				opcode, err = r.readLength()
				if err != nil {
					return err
				}
			}

			continue
		}
		/* Read key */
		key, err := r.readString()
		if err != nil {
			return err
		}
		fmt.Println(key)
		/* Read value */
		err = r.readObject(dtype)
		if err != nil {
			return err
		}
	}
}
func (r *RDB) verifyHeader(hd []byte) error {
	if !bytes.HasPrefix(hd, []byte("REDIS")) {
		return fmt.Errorf("bad ERSP HEADER %s", strconv.Quote(string(hd)))
	}

	version, err := strconv.ParseInt(string(hd[5:]), 10, 64)
	if err != nil {
		return err
	}
	log.Infof("当前Redis rdb 版本为：%d", version)
	r.version = int(version)
	return nil
}

func (r *RDB) readLength() (length uint64, err error) {
	var (
		data byte
	)
	data, err = r.rd.ReadByte()
	if err != nil {
		return
	}

	flag := (data & 0xc0) >> 6
	if flag == RDBEncVal {
		length = uint64(data & 0x3f)
		err = errors.New("length is encoding value, may not error")
		return
	}

	if flag == RDB6BitLen {
		length = uint64(data & 0x3f)
	} else if flag == RDB14BitLen {
		length = uint64(data&0x3f) << 8
		data, err = r.rd.ReadByte()
		if err != nil {
			return
		}
		length |= uint64(data)
	} else if data == RDB32BitLen {
		var l uint32
		err = binary.Read(r.rd, binary.BigEndian, &l)
		if err != nil {
			return
		}
		length = uint64(l)
	} else if data == RDB64BitLen {
		err = binary.Read(r.rd, binary.BigEndian, &length)
	}

	return
}

func (r *RDB) readString() (data []byte, err error) {
	return []byte(""), nil
}

func (r *RDB) readBinaryFloat() (interface{}, error) {
	return nil, nil
}

func (r *RDB) readBinaryDouble() (interface{}, error) {
	return nil, nil
}

func (r *RDB) readObject(b byte) error {
	return nil
}
