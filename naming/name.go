package naming

import (
	"fmt"
	"github.com/pister/yfs/common/bytesutil"
	"github.com/pister/yfs/common/hashutil"
	"github.com/pister/yfs/common/base64util"
)

// naming 长度 12 字节， base64转成字符串的时候不需要额外填充，转成后长度为16字节
type Name struct {
	NamePosition uint32
	NameBlockId  uint32
	RegionId     uint16
}

func (name *Name) String() string {
	buf := make([]byte, 12, 12)
	bytesutil.CopyUint32ToBytes(name.NamePosition, buf, 0)
	bytesutil.CopyUint32ToBytes(name.NameBlockId, buf, 4)
	bytesutil.CopyUint16ToBytes(name.RegionId, buf, 8)
	sumValue := hashutil.SumHash16(buf[:10])
	bytesutil.CopyUint16ToBytes(sumValue, buf, 10)
	return base64util.EncodeBase64ToString(buf)
}

func ParseNameFromString(strName string) (*Name, error) {
	data, err := base64util.DecodeBase64fromString(strName)
	if err != nil {
		return nil, err
	}
	sumValueFromCal := hashutil.SumHash16(data[:10])
	sumValueFromData := bytesutil.GetUint16FromBytes(data, 10)
	if sumValueFromCal != sumValueFromData {
		return nil, fmt.Errorf("sum validate fail")
	}
	name := new(Name)
	name.NamePosition = bytesutil.GetUint32FromBytes(data, 0)
	name.NameBlockId = bytesutil.GetUint32FromBytes(data, 4)
	name.RegionId = bytesutil.GetUint16FromBytes(data, 8)
	return name, nil
}
