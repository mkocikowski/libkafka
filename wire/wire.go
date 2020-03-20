// Package wire implements functions for marshaling and unmarshaling Kafka requests and responses.
package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strings"
)

var ord = binary.BigEndian

func Write(w io.Writer, val reflect.Value) error {
	switch val.Kind() {
	case reflect.Ptr, reflect.Interface:
		return Write(w, val.Elem())
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			name := val.Type().Field(i).Name
			if name[0:1] == strings.ToLower(name[0:1]) {
				continue // skip fields that start with lowercase
			}
			if val.Type().Field(i).Tag.Get("wire") == "omit" {
				continue
			}
			err := Write(w, val.Field(i))
			if err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice:
		if val.IsNil() {
			return binary.Write(w, ord, int32(-1))
		}
		l := int32(val.Len())
		if l == 0 {
			return binary.Write(w, ord, int32(0))
		}
		if err := binary.Write(w, ord, l); err != nil {
			return err
		}
		typ := val.Type().Elem()
		if typ.Kind() == reflect.Uint8 { // []byte
			_, err := w.Write(val.Bytes())
			return err
		}
		for i := 0; i < val.Len(); i++ {
			err := Write(w, val.Index(i))
			if err != nil {
				return err
			}
		}
		return nil
	case reflect.String:
		l := int16(val.Len())
		if l == 0 {
			//return binary.Write(w, ord, int16(-1))
			return binary.Write(w, ord, int16(0))
		}
		if err := binary.Write(w, ord, l); err != nil {
			return err
		}
		b := []byte(val.String())
		_, err := w.Write(b)
		return err
	case reflect.Int8:
		i := int8(val.Int())
		return binary.Write(w, ord, i)
	case reflect.Int16:
		i := int16(val.Int())
		return binary.Write(w, ord, i)
	case reflect.Int32:
		i := int32(val.Int())
		return binary.Write(w, ord, i)
	case reflect.Uint32:
		i := uint32(val.Uint())
		return binary.Write(w, ord, i)
	case reflect.Int64:
		i := int64(val.Int())
		return binary.Write(w, ord, i)
	case reflect.Bool:
		if val.Bool() {
			_, err := w.Write([]byte{1})
			return err
		}
		_, err := w.Write([]byte{0})
		return err
	}
	return nil
}

func Read(r io.Reader, val reflect.Value) error {
	//log.Println(val)
	switch val.Kind() {
	case reflect.Ptr, reflect.Interface:
		return Read(r, val.Elem())
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			name := val.Type().Field(i).Name
			if name[0:1] == strings.ToLower(name[0:1]) {
				continue // skip fields that start with lowercase
			}
			if val.Type().Field(i).Tag.Get("wire") == "omit" {
				continue
			}
			err := Read(r, val.Field(i))
			if err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice:
		var n int32
		if err := binary.Read(r, ord, &n); err != nil {
			return fmt.Errorf("error reading array length: %v", err)
		}
		typ := val.Type().Elem()
		if typ.Kind() == reflect.Uint8 { // []byte
			b := make([]byte, n)
			if _, err := io.ReadFull(r, b); err != nil {
				return fmt.Errorf("error reading []byte body: %v", err)
			}
			val.SetBytes(b)
			return nil
		}
		if int(n) == -1 {
			return nil // nil slice
		}
		val.Set(reflect.MakeSlice(val.Type(), 0, 0)) // empty slice
		for i := 0; i < int(n); i++ {
			element := reflect.New(typ).Elem()
			if err := Read(r, element); err != nil {
				return fmt.Errorf("error parsing array element: %v", err)
			}
			val.Set(reflect.Append(val, element))
		}
		return nil
	case reflect.String:
		var n int16
		if err := binary.Read(r, ord, &n); err != nil {
			return fmt.Errorf("error reading string length: %v", err)
		}
		if n < 0 {
			return nil
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(r, b); err != nil {
			return fmt.Errorf("error reading string body: %v", err)
		}
		val.SetString(string(b))
		return nil
	case reflect.Int8:
		var i int8
		if err := binary.Read(r, ord, &i); err != nil {
			return fmt.Errorf("error reading int8: %v", err)
		}
		val.SetInt(int64(i))
		return nil
	case reflect.Int16:
		var i int16
		if err := binary.Read(r, ord, &i); err != nil {
			return fmt.Errorf("error reading int16: %v", err)
		}
		val.SetInt(int64(i))
		return nil
	case reflect.Int32:
		var i int32
		if err := binary.Read(r, ord, &i); err != nil {
			return fmt.Errorf("error reading int32: %v", err)
		}
		val.SetInt(int64(i))
		return nil
	case reflect.Uint32:
		var i uint32
		if err := binary.Read(r, ord, &i); err != nil {
			return fmt.Errorf("error reading uint32: %v", err)
		}
		val.SetUint(uint64(i))
		return nil
	case reflect.Int64:
		var i int64
		if err := binary.Read(r, ord, &i); err != nil {
			return fmt.Errorf("error reading int64: %v", err)
		}
		val.SetInt(int64(i))
		return nil
	case reflect.Bool:
		b := make([]byte, 1)
		_, err := r.Read(b)
		if err != nil {
			return fmt.Errorf("error reading bool: %v", err)
		}
		val.SetBool(b[0] != 0)
		return nil
	}
	return nil
}
