__all__ = [
	"bool_",
	"int8",
	"int16",
	"int32",
	"int64",
	"uint8",
	"uint16",
	"uint32",
	"uint64",
	"float32",
	"float64",
	"timestamp",
	"duration",
	"string",
	"DataType",
	"BoolType",
	"Int8Type",
	"Int16Type",
	"Int32Type",
	"Int64Type",
	"UInt8Type",
	"UInt16Type",
	"UInt32Type",
	"UInt64Type",
	"Float32Type",
	"Float64Type",
	"TimestampType",
	"DurationType",
	"StringType",
]

class DataType: ...
class BoolType(DataType): ...
class Int8Type(DataType): ...
class Int16Type(DataType): ...
class Int32Type(DataType): ...
class Int64Type(DataType): ...
class UInt8Type(DataType): ...
class UInt16Type(DataType): ...
class UInt32Type(DataType): ...
class UInt64Type(DataType): ...
class Float32Type(DataType): ...
class Float64Type(DataType): ...
class TimestampType(DataType): ...
class DurationType(DataType): ...
class StringType(DataType): ...
bool_: BoolType
int8: Int8Type
int16: Int16Type
int32: Int32Type
int64: Int64Type
uint8: UInt8Type
uint16: UInt16Type
uint32: UInt32Type
uint64: UInt64Type
float32: Float32Type
float64: Float64Type
timestamp: TimestampType
duration: DurationType
string: StringType
