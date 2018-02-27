// Based on the interfaces from the Go authors, used in godoc:
// https://github.com/golang/tools/blob/master/godoc/vfs/vfs.go
package vfs

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
)

var ErrNoFile = errors.New("No such file")

// Easily testable interface for accessing the FileSystem.
type FileSystem interface {
	Open(name string) (ReadSeekCloser, error)
	Create(path string) (io.WriteCloser, error)
	Copy(destinationPath string, source io.Reader) error
	Move(sourcePath, destinationPath string) error
	Remove(path string) error
	Stat(path string) (os.FileInfo, error)
	Readdir(path string) ([]os.FileInfo, error)
	Mkdir(path string) error
	URL() *url.URL
}

// A ReadSeekCloser can Read, Seek, and Close.
type ReadSeekCloser interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

// Recursively creates a directory. If it fails part-way through creating the
// directories, it will not attempt to clean up.
func MkdirAll(fs FileSystem, path string) error {
	clean := pathpkg.Clean("/" + path)[1:]
	parts := strings.Split(clean, "/")

	for i := 1; i <= len(parts); i++ {
		dirName := "/" + pathpkg.Join(parts[0:i]...)
		if err := fs.Mkdir(dirName); err != nil {
			return err
		}
	}

	return nil
}

// Create a `FileSystem` where the root is some directory in another
// `FileSystem`. Filenames will be qualified so the underlying `FileSystem` can
// deal with absolute paths. A reasonable attempt is made to un-qualify
// filenames when exposed back to the consumer, but values may be able to slip
// through.
type subtree struct {
	fs   FileSystem
	root string
}

// Creates a `FileSystem` from the mount point of another `FileSystem`. This
// will check to make sure the mount point is a directory on creation.
func Subtree(fs FileSystem, root string) (FileSystem, error) {
	if root == "" {
		return fs, nil
	}
	mount, err := fs.Stat(root)
	if err != nil {
		return nil, err
	}
	if !mount.IsDir() {
		return nil, &os.PathError{
			Op:   "stat",
			Path: root,
			Err:  fmt.Errorf("Path '%s' is not a directory", root),
		}
	}

	return &subtree{fs, root}, nil
}

func (s *subtree) URL() *url.URL {
	url := s.fs.URL()
	url.Path = pathpkg.Join(url.Path, s.root)
	return url
}

func (s *subtree) Open(name string) (ReadSeekCloser, error) {
	r, err := s.fs.Open(s.mapPath(name))
	return r, s.unmapError(err)
}

func (s *subtree) Create(name string) (io.WriteCloser, error) {
	w, err := s.fs.Create(s.mapPath(name))
	return w, s.unmapError(err)
}

func (s *subtree) Copy(destPath string, source io.Reader) error {
	return s.fs.Copy(s.mapPath(destPath), source)
}

func (s *subtree) Move(srcPath, destPath string) error {
	return s.fs.Move(s.mapPath(srcPath), s.mapPath(destPath))
}

func (s *subtree) Remove(path string) error {
	return s.unmapError(s.fs.Remove(s.mapPath(path)))
}

func (s *subtree) Stat(path string) (os.FileInfo, error) {
	info, err := s.fs.Stat(s.mapPath(path))
	return info, s.unmapError(err)
}

func (s *subtree) Readdir(path string) ([]os.FileInfo, error) {
	infos, err := s.fs.Readdir(s.mapPath(path))
	return infos, s.unmapError(err)
}

func (s *subtree) Mkdir(path string) error {
	return s.unmapError(s.fs.Mkdir(s.mapPath(path)))
}

func (s *subtree) mapPath(path string) string {
	return filepath.Join(s.root, pathpkg.Clean(path))
}

func (s *subtree) unmapPath(path string) string {
	return strings.TrimPrefix(path, s.root)
}

func (s *subtree) unmapError(err error) error {
	switch t := err.(type) {
	default:
		return err
	case *os.PathError:
		return &os.PathError{
			Op:   t.Op,
			Path: s.unmapPath(t.Path),
			Err:  t.Err,
		}
	}
}

// Sorts a slice of `os.FileInfo` objects so that they're sorted by name. This
// is the interface the stdlib exposes, so it's the interface imposed on
// implementations
func sortFileInfos(fis []os.FileInfo) {
	sort.Sort(fileInfoSorter(fis))
}

type fileInfoSorter []os.FileInfo

func (fis fileInfoSorter) Len() int           { return len(fis) }
func (fis fileInfoSorter) Less(i, j int) bool { return fis[i].Name() < fis[j].Name() }
func (fis fileInfoSorter) Swap(i, j int)      { fis[i], fis[j] = fis[j], fis[i] }
