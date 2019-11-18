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
	handler *bufio.Reader

	version int

	expiry uint64
}

func NewRDB(handler *bufio.Reader) *RDB {
	r := &RDB{handler: handler}
	return r
}

func (r *RDB) Parse() (err error) {
	// 初始化
	r.expiry = 0

	// 获取  REDIS | RDB-VERSION
	headbuf := make([]byte, 9)
	_, err = io.ReadFull(r.handler, headbuf)
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

		/* Read type. */
		// 首先读出类型
		dtype, err := r.handler.ReadByte()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(dtype)

		/* Handle special types. */
		if dtype == RDBOpcodeExpireTime {
			/* EXPIRETIME: load an expire associated with the next key to load */
			err = binary.Read(r.handler, binary.LittleEndian, &r.expiry)
			if err != nil {
				return err
			}
			continue

		} else if dtype == RDBOpcodeExpireTimeMS {
			/* EXPIRETIME_MS: milliseconds precision expire times introduced */
			var expireSecond uint64
			err := binary.Read(r.handler, binary.LittleEndian, expireSecond)
			if err != nil {
				return err
			}
			r.expiry = expireSecond * 1000

			continue
		} else if dtype == RDBOpcodeFreq {
			/* FREQ: LFU frequency. */
			freq, err := r.handler.ReadByte()
			if err != nil {
				return err
			}

			fmt.Println("freq:", freq)

			continue
		} else if dtype == RDBOpcodeIdle {
			/* IDLE: LRU idle time. */
			idle, err := r.readLength()
			if err != nil {
				return err
			}
			fmt.Println("idle:", idle)
			continue
		} else if dtype == RDBOpcodeEOF {
			/* EOF: End of file, exit the main loop. */

			log.Infof("finish rdb reading of upstream")
			//  8字节的 CRC64 表示的文件校验和
			if r.version >= 5 {
				_, err := io.ReadFull(r.handler, headbuf[:8])
				if err != nil {
					return nil
				}
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
				return
			}
			expireSize, err := r.readLength()
			if err != nil {
				return
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
						return
					}
				case RDBModuleOpcodeFloat:
					_, err = r.readBinaryFloat()
					if err != nil {
						return
					}
				case RDBModuleOpcodeDouble:
					_, err = r.readBinaryDouble()
					if err != nil {
						return
					}
				case RDBModuleOpcodeString:
					_, err = r.readString()
					if err != nil {
						return
					}
				default:
					err = fmt.Errorf("unknown module opcode %d", opcode)
					return
				}

				opcode, err = r.readLength()
				if err != nil {
					return
				}
			}

			continue
		}
		/* Read key */
		key, err := r.readString()
		if err != nil {
			return err
		}
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
	fmt.Println(version)
	r.version = int(version)
	return nil
}

func (r *RDB) readLength() (length uint64, err error) {
	var (
		data byte
	)
	data, err = r.handler.ReadByte()
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
		data, err = r.handler.ReadByte()
		if err != nil {
			return
		}
		length |= uint64(data)
	} else if data == RDB32BitLen {
		var l uint32
		err = binary.Read(r.handler, binary.BigEndian, &l)
		if err != nil {
			return
		}
		length = uint64(l)
	} else if data == RDB64BitLen {
		err = binary.Read(r.handler, binary.BigEndian, &length)
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
