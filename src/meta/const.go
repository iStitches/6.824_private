package meta

import "strconv"

type Category uint8

const (
	// src/mr
	MR_DIR Category = 0x01
	// src/mrapps
	MRAPPS_DIR Category = 0x02
	// src/raft
	RAFT_DIR Category = 0x03
)

func (ec Category) String() string {
	switch ec {
	case MR_DIR:
		return "mr"
	case MRAPPS_DIR:
		return "mrapps"
	case RAFT_DIR:
		return "raft"
	default:
		return "CATEGORY " + strconv.Itoa(int(ec))
	}
}
