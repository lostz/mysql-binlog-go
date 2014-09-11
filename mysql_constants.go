package main

// These offset constants are based on v4 events
// sadly, golang doesn't support constant arrays (because of slices I think)
var BINLOG_MAGIC = [4]byte{0xfe, 0x62, 0x69, 0x6e}

// This is only int64 to save time on casting
// (hint: it probably shouldn't be int64)
const (
	EVENT_TYPE_OFFSET      int64 = 4
	EVENT_SERVER_ID_OFFSET       = 5
	EVENT_LEN_OFFSET             = 9
	EVENT_NEXT_OFFSET            = 13
	EVENT_FLAGS_OFFSET           = 17
	EVENT_EXTRA_OFFSET           = 19
)

const (
	UNKOWN_EVENT             byte = iota
	START_EVENT_V3
	QUERY_EVENT
	STOP_EVENT
	ROTATE_EVENT
	INTVAR_EVENT
	LOAD_EVENT
	SLAVE_EVENT
	CREATE_FILE_EVENT
	APPEND_BLOCK_EVENT
	EXEC_LOAD_EVENT
	DELETE_FILE_EVENT
	NEW_LOAD_EVENT
	RAID_EVENT
	USER_VAR_EVENT
	FORMAT_DESCRIPTION_EVENT
	XID_EVENT
	BEGIN_LOAD_QUERY_EVENT
	EXECUTE_LOAD_QUERY_EVENT
	TABLE_MAP_EVENT
	WRITE_ROWS_EVENTv0
	UPDATE_ROWS_EVENTv0
	DELETE_ROWS_EVENTv0
	WRITE_ROWS_EVENTv1
	UPDATE_ROWS_EVENTv1
	DELETE_ROWS_EVENTv1
	INCIDENT_EVENT
	HEARTBEAT_EVENT
	IGNORABLE_EVENT
	ROWS_QUERY_EVENT
	WRITE_ROWS_EVENTv2
	UPDATE_ROWS_EVENTv2
	DELETE_ROWS_EVENTv2
	GTID_EVENT
	ANONYMOUS_GTID_EVENT
	PREVIOUS_GTIDS_EVENT
)

const (
	MYSQL_TYPE_DECIMAL    byte = iota
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR
	MYSQL_TYPE_NEWDATE                       // Does not appear in binlog
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT
	MYSQL_TYPE_TIMESTAMP_V2
	MYSQL_TYPE_DATETIME_V2
	MYSQL_TYPE_TIME_V2
)

const (
	MYSQL_TYPE_NEWDECIMAL  byte = 246 + iota
	MYSQL_TYPE_ENUM                          // Does not appear in binlog
	MYSQL_TYPE_SET                           // Does not appear in binlog
	MYSQL_TYPE_TINY_BLOB                     // Does not appear in binlog
	MYSQL_TYPE_MEDIUM_BLOB                   // Does not appear in binlog
	MYSQL_TYPE_LONG_BLOB                     // Does not appear in binlog
	MYSQL_TYPE_BLOB
	MYSQL_TYPE_VAR_STRING
	MYSQL_TYPE_STRING
	MYSQL_TYPE_GEOMETRY
)

func checkBinlogMagic(magic []byte) bool {
	if len(magic) != len(BINLOG_MAGIC) {
		return false
	}

	for i, b := range magic {
		if b != BINLOG_MAGIC[i] {
			return false
		}
	}

	return true
}
