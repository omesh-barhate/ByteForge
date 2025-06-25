package types

const (
	TypeInt64  byte = 1
	TypeString byte = 2
	TypeByte   byte = 3
	TypeBool   byte = 4
	TypeInt32  byte = 5

	TypeWALEntry         byte = 20
	TypeWALLastIDItem    byte = 21
	TypeColumnDefinition byte = 90
	TypeRecord           byte = 100
	TypeDeletedRecord    byte = 101
	TypeHMap             byte = 220
	TypeHMapKey          byte = 221
	TypeHMapVal          byte = 222
	TypeList             byte = 230
	TypeIndex            byte = 240
	TypeIndexItem        byte = 241
	TypePage             byte = 255
)

const (
	LenByte  = 1
	LenInt32 = 4
	// LenMeta represents the "meta" bytes in each TLV record that accounts for type+len, for example: 1 8 0 0 0 || 10 0 0 0 0 0 0 0 0 the bytes before the || are the "meta" bytes and 10 ... is the actual value
	LenMeta uint32 = 5
)

type Scalar interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 | ~string
}
