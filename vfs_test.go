package vfs

import (
	"errors"
	"fmt"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Subtree", func() {
	var fs FileSystem

	BeforeEach(func() {
		fs = Mem(
			Dir("integration",
				Dir("directory",
					Dir("sub_directory"),
					File("child.txt", []byte("hi, child")),
				),
				Dir("empty_directory"),
				File("root.txt", []byte("hi, root")),
			),
			Dir("tree-two",
				Dir("directory",
					Dir("sub_directory"),
					File("child.txt", []byte("hi, child")),
				),
				Dir("empty_directory"),
				File("root.txt", []byte("hi, root")),
			),
		)
	})

	Describe("creation", func() {

		It("should create", func() {
			_, err := Subtree(fs, "/integration/directory")
			Expect(err).ToNot(HaveOccurred())
		})

		It("should error when given the root of a missing directory", func() {
			_, err := Subtree(fs, "/bad/dir")
			Expect(err).To(HaveOccurred())

			switch t := err.(type) {
			default:
				Fail(fmt.Sprintf("Unknown error type: %T", err))
			case *os.PathError:
				Expect(t.Op).To(Equal("stat"))
				Expect(t.Path).To(Equal("/bad/dir"))
			}
		})

		It("should error when given the root of a non-directory", func() {
			_, err := Subtree(fs, "/integration/root.txt")
			Expect(err).To(HaveOccurred())

			switch t := err.(type) {
			default:
				Fail(fmt.Sprintf("Unknown error type: %T", err))
			case *os.PathError:
				Expect(t.Op).To(Equal("stat"))
				Expect(t.Path).To(Equal("/integration/root.txt"))
				Expect(t.Error()).To(ContainSubstring("not a directory"))
			}
		})

	})

	Describe("stat", func() {

		It("should stat a file", func() {
			fs, err := Subtree(fs, "/tree-two/directory")
			Expect(err).ToNot(HaveOccurred())

			stat, err := fs.Stat("/child.txt")
			Expect(err).ToNot(HaveOccurred())

			Expect(stat.IsDir()).To(BeFalse())
		})

		It("should preserve filenames", func() {
			fs, _ := Subtree(fs, "/tree-two/directory")
			stat, _ := fs.Stat("/child.txt")

			Expect(stat.Name()).To(Equal("child.txt"))
		})

		It("should error if stat'ing a non-existant file", func() {
			stat, err := fs.Stat("/braap-braap-braaaaap.txt")

			Expect(stat).To(BeNil())
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(&os.PathError{
				Op:   "stat",
				Path: "/braap-braap-braaaaap.txt",
				Err:  errors.New("No such file"),
			}))
		})

	})
})

var _ = Describe("MkdirAll", func() {

	It("should create all directories", func() {
		fs := Mem()

		Expect(MkdirAll(fs, "/party/every/day")).To(Succeed())

		infos, _ := fs.Readdir("/")
		Expect(infos).To(HaveLen(1))
		Expect(infos[0].Name()).To(Equal("party"))
		Expect(infos[0].IsDir()).To(BeTrue())

		infos, _ = fs.Readdir("/party/every")
		Expect(infos).To(HaveLen(1))
		Expect(infos[0].Name()).To(Equal("day"))
		Expect(infos[0].IsDir()).To(BeTrue())

		infos, err := fs.Readdir("/party/every/day")
		Expect(err).ToNot(HaveOccurred())
		Expect(infos).To(HaveLen(0))
	})

})
