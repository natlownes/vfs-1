package vfs

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	pathpkg "path"
)

type osFS struct{}

var rootFs osFS

// Creates a `FileSystem` backed by files on disk. This implementation is based
// almost entirely off the work done by the Go team:
// https://github.com/golang/tools/blob/master/godoc/vfs/os.go
func OS(root string) (FileSystem, error) {
	return Subtree(rootFs, root)
}

func (root osFS) URL() *url.URL {
	return &url.URL{
		Scheme: "file",
		Path:   "/",
	}
}

func (root osFS) resolve(path string) string {
	// Ensure all paths are fully-qualified from the root of the FS
	return pathpkg.Clean("/" + path)
}

func (root osFS) Open(path string) (ReadSeekCloser, error) {
	f, err := os.Open(root.resolve(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, noFileErr(err.(*os.PathError))
		}
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.IsDir() {
		f.Close()
		return nil, fmt.Errorf("Open: %s is a directory", path)
	}
	return f, nil
}

func (root osFS) Remove(path string) error {
	err := os.Remove(root.resolve(path))
	if os.IsNotExist(err) {
		return noFileErr(err.(*os.PathError))
	}
	return err
}

func (root osFS) Create(path string) (io.WriteCloser, error) {
	file, err := os.Create(root.resolve(path))
	if e, ok := err.(*os.PathError); ok {
		e.Op = "create"
		return nil, e
	}
	return file, nil
}

func (root osFS) Copy(destPath string, source io.Reader) error {
	dest, err := root.Create(destPath)
	if err != nil {
		return err
	}

	if _, err := io.Copy(dest, source); err != nil {
		return err
	}

	return dest.Close()
}

func (root osFS) Move(srcPath, destPath string) error {
	return os.Rename(root.resolve(srcPath), root.resolve(destPath))
}

func (root osFS) Stat(path string) (os.FileInfo, error) {
	fi, err := os.Stat(root.resolve(path))
	if os.IsNotExist(err) {
		return nil, noFileErr(err.(*os.PathError))
	}
	return fi, err
}

func (root osFS) Mkdir(path string) error {
	return os.Mkdir(root.resolve(path), 0755)
}

func (root osFS) Readdir(path string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(root.resolve(path))
}

func noFileErr(pathErr *os.PathError) error {
	return &os.PathError{
		Op:   pathErr.Op,
		Path: pathErr.Path,
		Err:  ErrNoFile,
	}
}
