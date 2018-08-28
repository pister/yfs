package naming

import (
	"github.com/pister/yfs/utils"
	"fmt"
)

// naming 长度 12 字节， base64转成字符串的时候不需要额外填充，转成后长度为16字节
type Name struct {
	IndexPosition uint32
	IndexBlockId  uint32
	RegionId      uint16
}

func (name *Name) ToString() string {
	buf := make([]byte, 12, 12)
	utils.CopyUint32ToBytes(name.IndexPosition, buf, 0)
	utils.CopyUint32ToBytes(name.IndexBlockId, buf, 4)
	utils.CopyUint16ToBytes(name.RegionId, buf, 8)
	sumValue := utils.SumHash16(buf[:10])
	utils.CopyUint16ToBytes(sumValue, buf, 10)
	return utils.EncodeBase64ToString(buf)
}

func ParseNameFromString(strName string) (*Name, error) {
	data, err := utils.DecodeBase64fromString(strName)
	if err != nil {
		return nil, err
	}
	sumValueFromCal := utils.SumHash16(data[:10])
	sumValueFromData := utils.GetUint16FromBytes(data, 10)
	if sumValueFromCal != sumValueFromData {
		return nil, fmt.Errorf("sum validate fail")
	}
	name := new(Name)
	name.IndexPosition = utils.GetUint32FromBytes(data, 0)
	name.IndexBlockId = utils.GetUint32FromBytes(data, 4)
	name.RegionId = utils.GetUint16FromBytes(data, 8)
	return name, nil
}
