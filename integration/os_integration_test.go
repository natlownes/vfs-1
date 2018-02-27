package integration

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	. "github.com/onsi/gomega"

	. "github.com/natlownes/vfs"
)

type OsFsProvider struct {
	root string
}

func (fsp *OsFsProvider) Setup() {
	tmpdir, ok := os.LookupEnv("TEST_TMPDIR")
	var root string
	if ok {
		root = tmpdir
	} else {
		sandbox, err := ioutil.TempDir("", "vfs-tests")
		if err != nil {
			log.Fatal("failed to create tempdir")
		}
		root = sandbox
	}

	fsp.root = root

	Expect(
		os.Mkdir(
			path.Join(root, "directory"),
			0777)).To(Succeed())

	Expect(
		os.Mkdir(
			path.Join(root, "large_directory"),
			0777)).To(Succeed())

	i := 1
	for i <= 1100 {
		fileName := fmt.Sprintf("%04d", i)
		Expect(
			ioutil.WriteFile(
				path.Join(root, "large_directory", fileName),
				[]byte{},
				0666)).To(Succeed())
		i++
	}

	Expect(
		ioutil.WriteFile(
			path.Join(root, "directory", "child.txt"),
			[]byte("hi, child"),
			0666)).To(Succeed())

	Expect(
		os.Mkdir(
			path.Join(root, "directory", "sub_directory"),
			0777)).To(Succeed())

	Expect(
		os.Mkdir(
			path.Join(root, "empty_directory"),
			0777)).To(Succeed())

	Expect(
		ioutil.WriteFile(
			path.Join(root, "root.txt"),
			[]byte("hi, root"),
			0666)).To(Succeed())

	Expect(
		os.Mkdir(
			path.Join(root, "stat_test"),
			0777)).To(Succeed())

	Expect(
		os.Mkdir(
			path.Join(root, "stat_test1"),
			0777)).To(Succeed())
}

func (*OsFsProvider) Name() string {
	return "OS FS"
}

func (fsp *OsFsProvider) Create() FileSystem {
	fs, err := OS(fsp.root)
	Expect(err).ToNot(HaveOccurred())
	return fs
}

var _ = All(&OsFsProvider{})
