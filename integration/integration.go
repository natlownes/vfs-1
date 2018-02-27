// Tests compliance of a FileSystem. This assumes a directory as follows:
//
//  - /
//    + directory/
//    | + sub_directory/
//    | + child.txt content:(hi, child)
//    + empty_directory/
//    + large_directory/
//    | + 0001 (empty file)
//    | + 0002 (empty file)
//    | ...(continues until)
//    | + 1100 (empty file)
//    + root.txt content:(hi, root)
//    + stat_test/
//    + stat_test1/
package integration

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/natlownes/vfs"
)

var (
	numFilesExpected = 6
)

type FSProvider interface {
	Name() string
	Setup()
	Create() vfs.FileSystem
}

type setupOnce struct {
	fsp   FSProvider
	setup bool
}

func (s *setupOnce) Get() vfs.FileSystem {
	if !s.setup {
		s.fsp.Setup()
		s.setup = true
	}
	return s.fsp.Create()
}

func readDir(fsp *setupOnce) {
	var fs vfs.FileSystem

	Describe("Readdir", func() {

		BeforeEach(func() {
			fs = fsp.Get()
		})

		It("should list the contents of the root directory", func() {
			infos, err := fs.Readdir("/")
			Expect(err).ToNot(HaveOccurred())

			Expect(infos).To(HaveLen(numFilesExpected))

			Expect(infos[0].Name()).To(Equal("directory"))
			Expect(infos[0].IsDir()).To(BeTrue())

			Expect(infos[1].Name()).To(Equal("empty_directory"))
			Expect(infos[1].IsDir()).To(BeTrue())

			Expect(infos[3].Name()).To(Equal("root.txt"))
			Expect(infos[3].IsDir()).To(BeFalse())
		})

		It("should accept relative paths", func() {
			infos, err := fs.Readdir(".")
			Expect(err).ToNot(HaveOccurred())

			Expect(infos).To(HaveLen(numFilesExpected))
		})

		It("should list the contents of a child directory", func() {
			infos, err := fs.Readdir("/directory")
			Expect(err).ToNot(HaveOccurred())

			Expect(infos).To(HaveLen(2))

			Expect(infos[0].Name()).To(Equal("child.txt"))
			Expect(infos[0].IsDir()).To(BeFalse())

			Expect(infos[1].Name()).To(Equal("sub_directory"))
			Expect(infos[1].IsDir()).To(BeTrue())
		})

		It("should list the contents of an empty directory", func() {
			infos, err := fs.Readdir("/empty_directory")
			Expect(err).ToNot(HaveOccurred())

			Expect(infos).To(BeEmpty())
		})

		It("should not list the children on a non-existant directory", func() {
			_, err := fs.Readdir("/unreal")
			Expect(err).To(HaveOccurred())

			switch t := err.(type) {
			default:
				Fail(fmt.Sprintf("Expected *os.PathError, got %T", err))
			case *os.PathError:
				Expect(t.Op).To(Equal("open"))
				Expect(t.Path).To(Equal("/unreal"))
			}
		})

		It("should list all 1100 files of a large directory", func() {
			infos, err := fs.Readdir("/large_directory")
			Expect(err).ToNot(HaveOccurred())

			Expect(infos).To(HaveLen(1100))

			Expect(infos[0].Name()).To(Equal("0001"))
			Expect(infos[0].IsDir()).To(BeFalse())

			Expect(infos[1099].Name()).To(Equal("1100"))
			Expect(infos[1099].IsDir()).To(BeFalse())
		})

	})
}

func stat(fsp *setupOnce) {
	var fs vfs.FileSystem

	Describe("Stat", func() {

		BeforeEach(func() {
			fs = fsp.Get()
		})

		It("should stat a known file", func() {
			stat, err := fs.Stat("/root.txt")
			Expect(err).ToNot(HaveOccurred())

			Expect(stat.Name()).To(Equal("root.txt"))
			Expect(stat.IsDir()).To(BeFalse())
			Expect(stat.Size()).To(Equal(int64(8)))
		})

		It("should stat a known directory", func() {
			stat, err := fs.Stat("/directory")
			Expect(err).ToNot(HaveOccurred())

			Expect(stat.Name()).To(Equal("directory"))
			Expect(stat.IsDir()).To(BeTrue())
		})

		It("should stat a directory with a trailing slash", func() {
			stat, err := fs.Stat("/directory/")
			Expect(err).ToNot(HaveOccurred())

			Expect(stat.Name()).To(Equal("directory"))
			Expect(stat.IsDir()).To(BeTrue())
		})

		It("should fail to stat a missing file", func() {
			_, err := fs.Stat("missing-file")
			Expect(err).To(HaveOccurred())

			switch t := err.(type) {
			default:
				Fail(fmt.Sprintf("Expected *os.PathError, got %T", err))
			case *os.PathError:
				Expect(t.Op).To(Equal("stat"))
				Expect(t.Path).To(Equal("/missing-file"))
			}
		})

		It("should stat a file in a sub-directory", func() {
			stat, err := fs.Stat("/directory/child.txt")
			Expect(err).ToNot(HaveOccurred())

			Expect(stat.Name()).To(Equal("child.txt"))
			Expect(stat.IsDir()).To(BeFalse())
			Expect(stat.Size()).To(Equal(int64(9)))
		})

		It("should stat a file in a large sub-directory", func() {
			stat, err := fs.Stat("/large_directory/1100")
			Expect(err).ToNot(HaveOccurred())

			Expect(stat.Name()).To(Equal("1100"))
			Expect(stat.IsDir()).To(BeFalse())
			Expect(stat.Size()).To(Equal(int64(0)))
		})

		It("should fail to stat a missing file in a sub-directory", func() {
			_, err := fs.Stat("/directory/missing-file")
			Expect(err).To(HaveOccurred())

			switch t := err.(type) {
			default:
				Fail(fmt.Sprintf("Expected *os.PathError, got %T", err))
			case *os.PathError:
				Expect(t.Op).To(Equal("stat"))
				Expect(t.Path).To(Equal("/directory/missing-file"))
			}
		})

		It("should stat similarly named directories", func() {
			stat, err := fs.Stat("/stat_test")
			Expect(err).ToNot(HaveOccurred())

			Expect(stat.Name()).To(Equal("stat_test"))
			Expect(stat.IsDir()).To(BeTrue())
		})

	})
}

func open(fsp *setupOnce) {
	var fs vfs.FileSystem

	Describe("Open", func() {

		BeforeEach(func() {
			fs = fsp.Get()
		})

		It("should open a file", func() {
			r, err := fs.Open("root.txt")
			Expect(err).ToNot(HaveOccurred())

			bs, _ := ioutil.ReadAll(r)
			Expect(string(bs)).To(Equal("hi, root"))
		})

		It("should not open a missing file", func() {
			_, err := fs.Open("missing.txt")
			Expect(err).To(HaveOccurred())

			switch t := err.(type) {
			default:
				Fail(fmt.Sprintf("Expected *os.PathError, got %T", err))
			case *os.PathError:
				Expect(t.Op).To(Equal("open"))
				Expect(t.Path).To(Equal("/missing.txt"))
			}
		})

	})
}

func create(fsp *setupOnce) {
	var fs vfs.FileSystem

	Describe("Create", func() {

		BeforeEach(func() {
			fs = fsp.Get()
		})

		It("should return an io.WriteCloser and create on Close()", func() {
			w, err := fs.Create("root2.txt")
			Expect(err).ToNot(HaveOccurred())

			_, err = w.Write([]byte("Party city"))
			Expect(err).ToNot(HaveOccurred())
			err = w.Close()
			Expect(err).ToNot(HaveOccurred())

			defer fs.Remove("root2.txt")
			r, err := fs.Open("root2.txt")
			Expect(err).ToNot(HaveOccurred())

			infos, _ := fs.Readdir("/")
			Expect(infos[4].Name()).To(Equal("root2.txt"))
			bs, _ := ioutil.ReadAll(r)
			Expect(string(bs)).To(Equal("Party city"))
		})

		// This is skipped because it passes for os and mem implementations but not
		// for S3.
		XIt("should not create a in a missing directory", func() {
			_, err := fs.Open("whodat/missing.txt")
			Expect(err).To(HaveOccurred())

			switch t := err.(type) {
			default:
				Fail(fmt.Sprintf("Expected *os.PathError, got %T", err))
			case *os.PathError:
				Expect(t.Op).To(Equal("open"))
				Expect(t.Path).To(Equal("/whodat/missing.txt"))
			}

			_, err = fs.Create("whodat/missing.txt")
			Expect(err).To(HaveOccurred())

			switch t := err.(type) {
			default:
				Fail(fmt.Sprintf("Expected *os.PathError, got %T", err))
			case *os.PathError:
				Expect(t.Op).To(Equal("create"))
				// checking suffix here since each implementation won't have an
				// identical root path
				Expect(t.Path).To(HaveSuffix("/whodat/missing.txt"))
			}
		})

		It("should truncate the contents of an existing file at that path", func() {
			w, err := fs.Create("root2.txt")
			Expect(err).ToNot(HaveOccurred())
			defer fs.Remove("root2.txt")

			_, err = w.Write([]byte("Big Lots"))
			Expect(err).ToNot(HaveOccurred())
			err = w.Close()
			Expect(err).ToNot(HaveOccurred())

			r, err := fs.Open("root2.txt")
			Expect(err).ToNot(HaveOccurred())
			bs, _ := ioutil.ReadAll(r)
			Expect(string(bs)).To(Equal("Big Lots"))

			w, err = fs.Create("root2.txt")
			Expect(err).ToNot(HaveOccurred())
			_, err = w.Write([]byte("Jamesway"))
			Expect(err).ToNot(HaveOccurred())
			err = w.Close()
			Expect(err).ToNot(HaveOccurred())

			r, err = fs.Open("root2.txt")
			Expect(err).ToNot(HaveOccurred())
			bs, _ = ioutil.ReadAll(r)
			Expect(string(bs)).To(Equal("Jamesway"))
		})

		// This is skipped because it passes for s3 and mem implementations but not
		// for os.
		XIt("should not create a file until Close() is called", func() {
			w, err := fs.Create("jamesway_fires.txt")
			Expect(err).ToNot(HaveOccurred())

			_, err = fs.Stat("jamesway_fires.txt")
			Expect(err).To(HaveOccurred())

			err = w.Close()
			Expect(err).ToNot(HaveOccurred())
			defer fs.Remove("jamesway_fires.txt")

			_, err = fs.Stat("jamesway_fires.txt")
			Expect(err).NotTo(HaveOccurred())
		})

	})

}

func fsCopy(fsp *setupOnce) {
	var fs vfs.FileSystem

	Describe("Copy", func() {

		BeforeEach(func() {
			fs = fsp.Get()
		})

		It("should create a file at given path w/contents of reader", func() {
			_, err := fs.Stat("jamesway.txt")
			// file shouldn't exist
			Expect(err).To(HaveOccurred())

			source := bytes.NewReader([]byte("burning Jamesway sign"))

			err = fs.Copy("jamesway.txt", source)
			Expect(err).ToNot(HaveOccurred())
			defer fs.Remove("jamesway.txt")

			actual, err := fs.Open("jamesway.txt")
			Expect(err).ToNot(HaveOccurred())
			bs, _ := ioutil.ReadAll(actual)
			Expect(string(bs)).To(Equal("burning Jamesway sign"))
		})

	})
}

func fsMove(fsp *setupOnce) {
	var fs vfs.FileSystem

	Describe("Move", func() {

		BeforeEach(func() {
			fs = fsp.Get()
		})

		It("should move the file", func() {
			orig, err := fs.Stat("directory/child.txt")
			Expect(err).ToNot(HaveOccurred())

			err = fs.Move("directory/child.txt", "directory/sub_directory/child.txt")
			defer fs.Move("directory/sub_directory/child.txt", "directory/child.txt")
			Expect(err).ToNot(HaveOccurred())

			_, err = fs.Stat("directory/child.txt")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("stat /directory/child.txt: No such file"))

			moved, err := fs.Stat("directory/sub_directory/child.txt")
			Expect(err).ToNot(HaveOccurred())
			Expect(orig.Size()).To(Equal(moved.Size()))
		})

	})
}

func remove(fsp *setupOnce) {
	var fs vfs.FileSystem

	Describe("Remove", func() {

		BeforeEach(func() {
			fs = fsp.Get()
		})

		It("should remove files", func() {
			_, err := fs.Stat("new.txt")
			Expect(err).To(HaveOccurred())

			w, err := fs.Create("new.txt")
			Expect(err).ToNot(HaveOccurred())
			err = w.Close()
			Expect(err).ToNot(HaveOccurred())

			fi, err := fs.Stat("new.txt")
			Expect(err).ToNot(HaveOccurred())
			Expect(fi.Name()).To(Equal("new.txt"))

			Expect(fs.Remove("new.txt")).To(Succeed())

			_, err = fs.Stat("new.txt")
			Expect(err).To(HaveOccurred())
		})

		It("should not remove a non-existant file", func() {
			err := fs.Remove("missing.txt")
			Expect(err).To(HaveOccurred())

			switch t := err.(type) {
			default:
				Fail(fmt.Sprintf("Expected *os.PathError, got %T", err))
			case *os.PathError:
				Expect(t.Op).To(Equal("remove"))
				Expect(t.Path).To(Equal("/missing.txt"))
			}
		})

		It("should be able to remove the root directory", func() {
			Expect(fs.Mkdir("/new_root")).To(Succeed())
			infos, err := fs.Readdir("/")
			Expect(err).ToNot(HaveOccurred())
			Expect(infos).To(HaveLen(numFilesExpected + 1))

			st, err := vfs.Subtree(fs, "/new_root")
			infos, err = st.Readdir("/")
			Expect(err).ToNot(HaveOccurred())
			Expect(infos).To(HaveLen(0))

			Expect(st.Remove("")).To(Succeed())
			infos, err = fs.Readdir("/")
			Expect(err).ToNot(HaveOccurred())
			Expect(infos).To(HaveLen(numFilesExpected))
		})

	})

}

func mkdir(fsp *setupOnce) {
	var fs vfs.FileSystem

	Describe("Mkdir", func() {

		BeforeEach(func() {
			fs = fsp.Get()
		})

		It("should make a directory", func() {
			Expect(fs.Mkdir("/var")).To(Succeed())
			defer func() {
				Expect(fs.Remove("/var")).To(Succeed())
			}()

			info, err := fs.Stat("/var")
			Expect(err).ToNot(HaveOccurred())

			Expect(info.Name()).To(Equal("var"))
			Expect(info.IsDir()).To(BeTrue())
		})

		It("should make a directory within an existing directory", func() {
			Expect(fs.Mkdir("/stat_test/directory")).To(Succeed())
			defer func() {
				Expect(fs.Remove("/stat_test/directory")).To(Succeed())
			}()

			info, err := fs.Stat("/stat_test/directory")
			Expect(err).ToNot(HaveOccurred())

			Expect(info.Name()).To(Equal("directory"))
			Expect(info.IsDir()).To(BeTrue())
		})
	})
}

func fileOperations(fsp *setupOnce) {

	filename := "closed.txt"

	var (
		err error
		f   io.WriteCloser
		fs  vfs.FileSystem
	)

	Describe("File", func() {

		BeforeEach(func() {
			fs = fsp.Get()
			f, err = fs.Create(filename)
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			err = fs.Remove(filename)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should error on write to closed file", func() {
			err = f.Close()
			Expect(err).ToNot(HaveOccurred())
			_, err = f.Write([]byte("Fuuuuuuuuuuuuu"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("file already closed"))
		})

		It("should error on close to closed file", func() {
			err = f.Close()
			Expect(err).ToNot(HaveOccurred())
			err = f.Close()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("file already closed"))
		})
	})
}

func All(fsp FSProvider) bool {
	once := &setupOnce{fsp: fsp}

	Describe(fsp.Name(), func() {
		readDir(once)
		stat(once)
		open(once)
		fsCopy(once)
		fsMove(once)
		remove(once)
		create(once)
		mkdir(once)
		fileOperations(once)
	})

	return true
}
