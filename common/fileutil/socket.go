package fileutil

import "net"

func WriteFully(conn net.Conn, data []byte) error {
	for {
		remainLen := len(data)
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		if n == remainLen {
			return nil
		}
		data = data[n:]
	}
	return nil
}

func ReadFully(conn net.Conn, buf []byte) error {
	for {
		remainLen := len(buf)
		n, err := conn.Read(buf)
		if err != nil {
			return err
		}
		if n == remainLen {
			return nil
		}
		buf = buf[n:]
	}
	return nil
}
