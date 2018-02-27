package vfs

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Walk", func() {

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
			Dir("tree-3",
				Dir("1",
					Dir("2",
						Dir("6",
							File("8.txt", []byte("yo")),
						),
						Dir("3",
							Dir("4",
								File("5.txt", []byte("yo")),
							),
						),
					),
				),
			),
		)
	})

	It("should call walkFn for each directory & file", func() {
		count := 0
		err := Walk(fs, func(fs FileSystem, info os.FileInfo, err error) error {
			count = count + 1
			return err
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(20))
	})

	It("should be able to Stat each file by name and subtree", func() {
		err := Walk(fs, func(tree FileSystem, info os.FileInfo, err error) error {
			_, err = tree.Stat(info.Name())
			Expect(err).NotTo(HaveOccurred())
			return nil
		})

		Expect(err).NotTo(HaveOccurred())
	})

})
