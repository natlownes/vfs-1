package vfs

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	pathpkg "path"
	"strings"
	"time"
)

// Convenience function for creating a memory `FileSystem`
func Mem(children ...*MemNode) FileSystem {
	return Dir("", children...)
}

// Convenience function for creating a directory in memory
func Dir(name string, children ...*MemNode) *MemNode {
	return &MemNode{
		name:     name,
		modTime:  time.Now(),
		isDir:    true,
		children: children,
	}
}

func File(name string, content []byte) *MemNode {
	return &MemNode{
		name:    name,
		modTime: time.Now(),
		isDir:   false,
		content: content,
	}
}

func FileWithModTime(name string, content []byte, mtime time.Time) *MemNode {
	node := File(name, content)
	node.modTime = mtime
	return node
}

type MemNode struct {
	name     string
	isDir    bool
	modTime  time.Time
	content  []byte
	children []*MemNode
}

type memFile struct {
	closed  bool
	content bytes.Buffer
	dir     *MemNode
	path    string
}

func (mf *memFile) Write(p []byte) (int, error) {
	if mf.closed {
		return 0, os.ErrClosed
	}
	return mf.content.Write(p)
}

func (mf *memFile) Close() error {
	if mf.closed {
		return os.ErrClosed
	}
	mf.dir.children = append(mf.dir.children, &MemNode{
		name:    pathpkg.Base(mf.path),
		content: mf.content.Bytes(),
		modTime: time.Now(),
	})
	mf.closed = true
	return nil
}

func (*MemNode) URL() *url.URL {
	return &url.URL{
		Scheme: "mem",
		Path:   "/",
	}
}

func (mn *MemNode) IsDir() bool {
	return mn.isDir
}

func (mn *MemNode) ModTime() time.Time {
	return mn.modTime
}

func (mn *MemNode) Name() string {
	return mn.name
}

func (mn *MemNode) Size() int64 {
	return int64(len(mn.content))
}

func (mn *MemNode) Mode() os.FileMode {
	if mn.isDir {
		return os.ModeDir
	}
	return os.FileMode(0)
}

func (*MemNode) Sys() interface{} {
	return nil
}

func (mn *MemNode) Content() ReadSeekCloser {
	r := bytes.NewReader(mn.content)
	return &ByteReaderCloser{r}
}

func (mn *MemNode) Open(path string) (ReadSeekCloser, error) {
	path = pathpkg.Clean("/" + path)

	child := mn.childByPath(path)
	if child == nil {
		return nil, &os.PathError{"open", path, ErrNoFile}
	}

	return child.Content(), nil
}

func (mn *MemNode) Remove(path string) error {
	path = pathpkg.Clean("/" + path)
	base := pathpkg.Base(path)
	dir := mn.parentNode(path)

	if dir == nil || !dir.isDir {
		return &os.PathError{
			Op:   "remove",
			Path: path,
			Err:  fmt.Errorf("No parent directory %s", path),
		}
	}

	var children []*MemNode
	for _, child := range dir.children {
		if child.name != base {
			children = append(children, child)
		}
	}

	// If we end up with the same number of children, this file doesn't exist to
	// remove
	if len(children) == len(dir.children) {
		return &os.PathError{Op: "remove", Path: path, Err: ErrNoFile}
	}

	dir.children = children
	return nil
}

func (mn *MemNode) Create(path string) (io.WriteCloser, error) {
	path = pathpkg.Clean("/" + path)
	parent := pathpkg.Dir(path)
	dir := mn.childByPath(parent)

	if dir == nil || !dir.isDir {
		return nil, &os.PathError{
			Op:   "create",
			Path: path,
			Err:  fmt.Errorf("No parent directory %s", parent),
		}
	}

	// Remove any existing file with the same name
	if err := mn.Remove(path); err != nil {
		// If the error is just that the file doesnt exist, ignore it
		if pe, ok := err.(*os.PathError); !ok || pe.Err != ErrNoFile {
			return nil, err
		}
	}

	return &memFile{
		path: pathpkg.Base(path),
		dir:  dir,
	}, nil
}

func (mn *MemNode) Copy(destPath string, source io.Reader) error {
	dest, err := mn.Create(destPath)
	if err != nil {
		return err
	}

	if _, err := io.Copy(dest, source); err != nil {
		return err
	}

	return dest.Close()
}

func (mn *MemNode) Move(srcPath, destPath string) error {
	src := mn.parentNode(srcPath)
	dest := mn.parentNode(destPath)

	var file *MemNode
	var fileIndex int
	for i, c := range src.children {
		if c.name == pathpkg.Base(srcPath) {
			file = c
			fileIndex = i
			break
		}
	}
	if file == nil {
		return &os.PathError{Op: "move", Path: srcPath, Err: ErrNoFile}
	}

	src.children = append(src.children[:fileIndex], src.children[fileIndex+1:]...)
	dest.children = append(dest.children, file)

	return nil
}

func (mn *MemNode) Stat(path string) (os.FileInfo, error) {
	path = pathpkg.Clean("/" + path)
	child := mn.childByPath(path)

	if child == nil {
		return nil, &os.PathError{"stat", path, ErrNoFile}
	}
	return child, nil
}

func (mn *MemNode) Readdir(path string) ([]os.FileInfo, error) {
	node := mn.childByPath(path)
	if node == nil {
		return nil, &os.PathError{"open", path, ErrNoFile}
	}
	children := make([]os.FileInfo, len(node.children))
	for i, child := range node.children {
		children[i] = child
	}

	sortFileInfos(children)
	return children, nil
}

func (mn *MemNode) Mkdir(path string) error {
	name := pathpkg.Base(path)
	dir := mn.parentNode(path)

	if dir == nil || !dir.isDir {
		return &os.PathError{
			Op:   "mkdir",
			Path: path,
			Err:  fmt.Errorf("No parent directory for %s", path),
		}
	}

	dir.children = append(dir.children, Dir(name))
	return nil
}

func (mn *MemNode) childByName(name string) *MemNode {
	for _, child := range mn.children {
		if child.name == name {
			return child
		}
	}

	return nil
}

func (mn *MemNode) childByPath(path string) *MemNode {
	clean := pathpkg.Clean("/" + path)[1:]
	return mn.child(strings.Split(clean, "/"))
}

func (mn *MemNode) child(parts []string) *MemNode {
	if len(parts) == 0 {
		return mn
	}
	if len(parts) == 1 && parts[0] == "" {
		return mn
	}

	child := mn.childByName(parts[0])
	if child == nil {
		return nil
	}
	return child.child(parts[1:])
}

func (mn *MemNode) parentNode(path string) *MemNode {
	path = pathpkg.Clean("/" + path)
	return mn.childByPath(pathpkg.Dir(path))
}

type ByteReaderCloser struct {
	*bytes.Reader
}

func (brc ByteReaderCloser) Close() error {
	return nil
}
