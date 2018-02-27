package s3fs

import (
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/natlownes/vfs"
)

// `FileSystem` backed by S3
type S3FileSystem struct {
	s3         *s3.S3
	acl        *string
	bucket     *string
	tmpDir     string
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader
}

// Create a new `FileSystem` from the given AWS session and bucket and accept
// functional options to modify that `FileSystem`
func New(
	sess *session.Session,
	bucket string,
	opts ...func(*S3FileSystem),
) vfs.FileSystem {

	s3Client := s3.New(sess)
	s3FileSystem := &S3FileSystem{
		s3:         s3Client,
		downloader: s3manager.NewDownloaderWithClient(s3Client),
		uploader:   s3manager.NewUploaderWithClient(s3Client),
		tmpDir:     os.TempDir(),
		bucket:     aws.String(bucket),
	}
	for _, opt := range opts {
		opt(s3FileSystem)
	}
	return s3FileSystem
}

func ACL(acl string) func(*S3FileSystem) {
	return func(fs *S3FileSystem) {
		fs.acl = aws.String(acl)
	}
}

func (s3fs *S3FileSystem) URL() *url.URL {
	return &url.URL{
		Scheme: "s3",
		Host:   *s3fs.bucket,
		Path:   "/",
	}
}

type s3File struct {
	tmp  *os.File
	s3fs *S3FileSystem
	path string
}

func (f *s3File) Write(p []byte) (int, error) {
	return f.tmp.Write(p)
}

func (f *s3File) Close() error {
	if _, err := f.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}

	key := f.s3fs.keyPath(f.path)
	_, err := f.s3fs.uploader.Upload(&s3manager.UploadInput{
		ACL:         f.s3fs.acl,
		Body:        f.tmp,
		Bucket:      f.s3fs.bucket,
		ContentType: aws.String(guessMimeTypeFromKey(key)),
		Key:         aws.String(key),
	})

	if err != nil {
		return s3Err("create", key, err)
	}

	return f.tmp.Close()
}

// Removes an object from S3. Note that S3 will gladly delete a non-existant
// object and return no error. This does a `Stat` before deleting to keep the
// interface the same as other `FileSystem`s. If `Stat` returns a directory, a
// '/' will be appended to the path to match the S3 key
func (s3fs *S3FileSystem) Remove(path string) error {
	key := s3fs.keyPath(path)

	if fi, err := s3fs.Stat(path); err != nil {
		if pe, ok := err.(*os.PathError); ok {
			pe.Op = "remove"
		}
		return err
	} else if fi.IsDir() {
		key = key + "/"
	}

	_, err := s3fs.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: s3fs.bucket,
		Key:    aws.String(key),
	})

	return s3Err("remove", key, err)
}

// Creates a local file and uses the tmp file as the backing store for the
// returned s3File.  when the s3File is closed it's uploaded to S3
func (s3fs *S3FileSystem) Create(path string) (io.WriteCloser, error) {
	tmp, err := unlinkedTempFile(s3fs.tmpDir, pathpkg.Base(path))
	if err != nil {
		return nil, err
	}

	return &s3File{
		tmp:  tmp,
		s3fs: s3fs,
		path: path,
	}, nil
}

// Copy will take an io.Reader and upload it directly to S3
func (s3fs *S3FileSystem) Copy(destPath string, source io.Reader) error {
	key := s3fs.keyPath(destPath)
	_, err := s3fs.uploader.Upload(&s3manager.UploadInput{
		ACL:         s3fs.acl,
		Body:        source,
		Bucket:      s3fs.bucket,
		ContentType: aws.String(guessMimeTypeFromKey(key)),
		Key:         aws.String(key),
	})

	if err != nil {
		return s3Err("copy", key, err)
	}
	return nil
}

// Move will do an S3-to-S3 copy and remove the original
func (s3fs *S3FileSystem) Move(srcPath, destPath string) error {
	srcKey := s3fs.keyPath(srcPath)
	destKey := s3fs.keyPath(destPath)

	if _, err := s3fs.s3.CopyObject(&s3.CopyObjectInput{
		ACL:        s3fs.acl,
		Bucket:     s3fs.bucket,
		CopySource: aws.String(fmt.Sprintf("%s/%s", *s3fs.bucket, srcKey)),
		Key:        aws.String(destKey),
	}); err != nil {
		return s3Err("move", destKey, err)
	}

	return s3Err("move", destKey, s3fs.Remove(srcPath))
}

// Returns a file for reading. The caller is responsible for closing.
func (s3fs *S3FileSystem) Open(path string) (vfs.ReadSeekCloser, error) {
	req := &s3.GetObjectInput{
		Bucket: s3fs.bucket,
		Key:    aws.String(s3fs.keyPath(path)),
	}
	tmp, err := unlinkedTempFile(s3fs.tmpDir, pathpkg.Base(path))
	if err != nil {
		return nil, err
	}
	if _, err = s3fs.downloader.Download(tmp, req); err != nil {
		tmp.Close()
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "NoSuchKey" {
			return nil, s3Err("open", *req.Key, vfs.ErrNoFile)
		}
		return nil, s3Err("open", *req.Key, err)
	}
	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		tmp.Close()
		return nil, err
	}
	return tmp, nil
}

// S3 has no directories. This will follow the general convention of creating an
// empty file at the path with a trailing '/' in the name.
func (s3fs *S3FileSystem) Mkdir(path string) error {
	key := s3fs.keyPath(path) + "/"

	_, err := s3fs.s3.PutObject(&s3.PutObjectInput{
		ACL:    s3fs.acl,
		Bucket: s3fs.bucket,
		Key:    aws.String(key),
	})

	return err
}

// Stats a path. S3 has no real concept of directories, so it must do a list
// operation with a prefix.  Heuristically determines if the key is a directory
// by seeing if it ends with a slash.
// Since we're not making the list request with a MaxKeys option we could find
// ourselves iterating over a ridiculous amount of keys if we stat a path like:
// "i" where there are a lot of keys that begin with "i".
func (s3fs *S3FileSystem) Stat(path string) (os.FileInfo, error) {
	key := s3fs.keyPath(path)

	req := &s3.ListObjectsV2Input{
		Bucket:    s3fs.bucket,
		Delimiter: aws.String("/"),
		Prefix:    aws.String(key),
	}

	var respCommonPrefixes []*s3.CommonPrefix
	var respContents []*s3.Object
	err := s3fs.s3.ListObjectsV2Pages(req,
		func(page *s3.ListObjectsV2Output, _ bool) bool {
			respCommonPrefixes = append(respCommonPrefixes, page.CommonPrefixes...)
			respContents = append(respContents, page.Contents...)
			return true
		},
	)

	if err != nil {
		return nil, s3Err("stat", key, err)
	}

	// Look for a directory
	expectedDir := key + "/"
	for _, prefix := range respCommonPrefixes {
		if *prefix.Prefix == expectedDir {
			fileInfo := &s3FileInfo{
				name:  pathpkg.Base(*prefix.Prefix),
				isDir: true,
			}
			return fileInfo, nil
		}
	}

	// Look for a file
	for _, obj := range respContents {
		if *obj.Key == key {
			fileInfo := &s3FileInfo{
				name:    pathpkg.Base(*obj.Key),
				size:    *obj.Size,
				modTime: *obj.LastModified,
			}
			return fileInfo, nil
		}
	}

	return nil, s3Err("stat", key, vfs.ErrNoFile)
}

// Reads keys off S3 with a key prefixed by the given path, but no trailing '/'.
// Results will be ordered by name
func (s3fs *S3FileSystem) Readdir(path string) ([]os.FileInfo, error) {
	key := s3fs.keyPath(path)
	if !strings.HasSuffix(key, "/") && key != "" {
		key += "/"
	}

	req := &s3.ListObjectsV2Input{
		Bucket:    s3fs.bucket,
		Delimiter: aws.String("/"),
		Prefix:    aws.String(key),
	}

	var dirs []*s3.CommonPrefix
	var files []*s3.Object
	err := s3fs.s3.ListObjectsV2Pages(req,
		func(page *s3.ListObjectsV2Output, _ bool) bool {
			dirs = append(dirs, page.CommonPrefixes...)
			files = append(files, page.Contents...)
			return true
		},
	)

	if err != nil {
		return nil, err
	}

	var infos s3FileInfos
	if len(dirs) == 0 && len(files) == 0 {
		return nil, s3Err("open", key, vfs.ErrNoFile)
	}

	for _, dir := range dirs {
		name := strings.TrimSuffix(*dir.Prefix, "/")
		name = strings.TrimPrefix(name, *req.Prefix)

		infos = append(infos, &s3FileInfo{name: name, isDir: true})
	}
	for _, file := range files {
		fileKey := strings.Replace(*file.Key, *req.Prefix, "", 1)

		if fileKey != "" {
			infos = append(infos, &s3FileInfo{
				name:    fileKey,
				size:    *file.Size,
				modTime: *file.LastModified,
				sys:     file,
			})
		}
	}

	sort.Sort(infos)
	fileInfos := make([]os.FileInfo, len(infos))
	for i, info := range infos {
		fileInfos[i] = info
	}
	return fileInfos, nil
}

func (s3fs *S3FileSystem) keyPath(path string) string {
	return strings.TrimPrefix(pathpkg.Clean("/"+path), "/")
}

func s3Err(op, key string, err error) error {
	if err == nil {
		return nil
	}
	return &os.PathError{
		Op:   op,
		Path: strings.TrimSuffix("/"+key, "/"),
		Err:  err,
	}
}

// Creates a temp file and immediately removes it. Assuming a POSIX OS, the file
// will stay on disk as long as the process keeps a handle open. When the handle
// is closed or the process crashes, the OS may free the space.
func unlinkedTempFile(dir, prefix string) (*os.File, error) {
	file, err := ioutil.TempFile(dir, prefix)
	if err == nil {
		err = os.Remove(file.Name())
		if err != nil {
			file.Close()
		}
	}
	return file, err
}

type s3FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
}

func (fi *s3FileInfo) Name() string       { return fi.name }
func (fi *s3FileInfo) Size() int64        { return fi.size }
func (fi *s3FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *s3FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *s3FileInfo) IsDir() bool        { return fi.isDir }
func (fi *s3FileInfo) Sys() interface{}   { return fi.sys }

type s3FileInfos []*s3FileInfo

func (s3fs s3FileInfos) Len() int { return len(s3fs) }
func (s3fs s3FileInfos) Less(i, j int) bool {
	return s3fs[i].Name() < s3fs[j].Name()
}
func (s3fs s3FileInfos) Swap(i, j int) {
	s3fs[i], s3fs[j] = s3fs[j], s3fs[i]
}

func guessMimeTypeFromKey(key string) string {
	return mime.TypeByExtension(filepath.Ext(key))
}
