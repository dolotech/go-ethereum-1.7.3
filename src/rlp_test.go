package main

import (
	"math/big"
	"testing"
	"github.com/ethereum/go-ethereum/rlp"
	"fmt"
	"bytes"
)

/*
rlp包用法
rlp目的是可以将常用的数据结构,uint,string,[]byte,struct,slice,array,big.int等序列化以及反序列化.
要注意的是rlp特别不支持有符号数的序列化
具体用法见下
*/
//编码
type TestRlpStruct struct {
	A      uint
	B      string
	C      []byte
	BigInt *big.Int
}

//rlp用法
func TestRlp(t *testing.T) {
	//1.将一个整数数组序列化
	arrdata, err := rlp.EncodeToBytes([]uint{32, 28})
	fmt.Printf("unuse err:%v\n", err)
	//fmt.Sprintf("data=%s,err=%v", hex.EncodeToString(arrdata), err)
	//2.将数组反序列化
	var intarray []uint
	err = rlp.DecodeBytes(arrdata, &intarray)
	//intarray 应为{32,28}
	fmt.Printf("intarray=%v\n", intarray)

	//3.将一个布尔变量序列化到一个writer中
	writer := new(bytes.Buffer)
	err = rlp.Encode(writer, true)
	//fmt.Sprintf("data=%s,err=%v",hex.EncodeToString(writer.Bytes()),err)
	//4.将一个布尔变量反序列化
	var b bool
	err = rlp.DecodeBytes(writer.Bytes(), &b)
	//b:true
	fmt.Printf("b=%v\n", b)

	//5.将任意一个struct序列化
	//将一个struct序列化到reader中
	_, r, err := rlp.EncodeToReader(TestRlpStruct{3, "44", []byte{0x12, 0x32}, big.NewInt(32)})
	var teststruct TestRlpStruct
	err = rlp.Decode(r, &teststruct)
	//{A:0x3, B:"44", C:[]uint8{0x12, 0x32}, BigInt:32}
	fmt.Printf("teststruct=%#v\n", teststruct)
	ba,err:=rlp.EncodeToBytes(&TestRlpStruct{3, "44", []byte{0x12, 0x32}, big.NewInt(32)})
	fmt.Println("teststruct",ba)

}