package Ne

import (
	"encoding/binary"
	"encoding/hex"
	"net"
	"path"
)

func IP(cidr string, id uint64) (net.IP, error) {
	ip, ipNet, err := net.ParseCIDR(cidr)

	if err != nil {
		return nil, err
	}

	if len(ip) != net.IPv6len {
		return nil, &net.ParseError{Type: "IPv6 address", Text: cidr}
	}

	if ones, _ := ipNet.Mask.Size(); ones > 64 {
		return nil, &net.ParseError{Type: "CIDR address", Text: cidr}
	}

	binary.LittleEndian.PutUint64(ip[8:], id)

	return ip, nil
}

func Path(dir string, id uint64) string {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, id)

	name := hex.EncodeToString(b)

	if len(dir) == 0 {
		return name + ".sock"
	}

	return path.Join(dir, name+".sock")
}
