package integration

import (
	"fmt"

	. "github.com/onsi/gomega"

	. "github.com/natlownes/vfs"
)

type SubtreeProvider struct {
	fs FileSystem
}

func (stp *SubtreeProvider) Setup() {
	largeDirFiles := make([]*MemNode, 1100)
	for i := range largeDirFiles {
		largeDirFiles[i] = File(fmt.Sprintf("%04d", i+1), []byte{})
	}
	stp.fs = Mem(
		Dir("integration",
			Dir("directory",
				Dir("sub_directory"),
				File("child.txt", []byte("hi, child"))),
			Dir("empty_directory"),
			Dir("stat_test"),
			Dir("stat_test1"),
			File("root.txt", []byte("hi, root")),
			Dir("large_directory", largeDirFiles...)),
		Dir("tree-two",
			Dir("directory",
				Dir("sub_directory"),
				File("child.txt", []byte("hi, child"))),
			Dir("empty_directory"),
			File("root.txt", []byte("hi, root"))))
}

func (*SubtreeProvider) Name() string {
	return "Subtree"
}

func (stp *SubtreeProvider) Create() FileSystem {
	st, err := Subtree(stp.fs, "/integration")
	Expect(err).ToNot(HaveOccurred())
	return st
}

var _ = All(&SubtreeProvider{})
