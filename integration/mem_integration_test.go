package integration

import (
	"fmt"

	. "github.com/natlownes/vfs"
)

type MemFSProvider struct{}

func (MemFSProvider) Setup() {}

func (MemFSProvider) Name() string {
	return "MemFS"
}

func (MemFSProvider) Create() FileSystem {
	largeDirFiles := make([]*MemNode, 1100)
	for i := range largeDirFiles {
		largeDirFiles[i] = File(fmt.Sprintf("%04d", i+1), []byte{})
	}
	return Mem(
		Dir("directory",
			Dir("sub_directory"),
			File("child.txt", []byte("hi, child")),
		),
		Dir("empty_directory"),
		Dir("stat_test"),
		Dir("stat_test1"),
		File("root.txt", []byte("hi, root")),
		Dir("large_directory", largeDirFiles...),
	)
}

var _ = All(MemFSProvider{})
