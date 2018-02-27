package s3fs

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ACL", func() {
	It("should add acl option to S3FileSystem as a string pointer", func() {
		s3FileSystem := &S3FileSystem{}
		ACL("public-read")(s3FileSystem)
		Expect(*s3FileSystem.acl).To(Equal("public-read"))
	})
})
