package vfs

import (
	"os"
)

type WalkFunc func(fs FileSystem, info os.FileInfo, err error) error

func Walk(fs FileSystem, walkFn WalkFunc) error {
	infos, err := fs.Readdir(".")
	if err != nil {
		return err
	}
	for _, info := range infos {
		walkFn(fs, info, err)
		if info.IsDir() {
			if tree, err := Subtree(fs, info.Name()); err == nil {
				Walk(tree, walkFn)
			} else {
				return err
			}
		}
	}
	return nil
}
