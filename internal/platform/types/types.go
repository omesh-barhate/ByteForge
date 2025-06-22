package types

const (
	TypeInt64  byte = 1
	TypeString byte = 2
	TypeByte   byte = 3
	TypeBool   byte = 4
	TypeInt32  byte = 5

	TypeWALEntry         byte = 20
	TypeWALLastIDItem    byte = 21
	TypeColumnDefinition byte = 99
	TypeRecord           byte = 100
	TypeDeletedRecord    byte = 101
)

const (
	LenByte  = 1
	LenInt32 = 4
	LenInt64 = 8
	LenMeta  = 5
)
