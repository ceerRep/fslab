/*
Filesystem Lab disigned and implemented by ceerRep
*/

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 29
#define _FILE_OFFSET_BITS 64
#endif

extern "C"
{
#include "disk.h"
}
#include <cassert>
#include <cstddef>
#include <errno.h>
#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <functional>
#include <new>

#define S_IFDIR 0040000
#define S_IFREG 0100000
#define DIRMODE S_IFDIR | 0755
#define REGMODE S_IFREG | 0644

#include "fs.hpp"

//Format the virtual block device in the following function
int mkfs()
{
    Disk::mkfs();
    return 0;
}

int get_file_in_inode(int inodeno, std::string filename)
{
    auto directory = DirectoryProxy(inodeno);
    int length = directory.length();

    for (int i = 0; i < length; i++)
    {
        auto file = directory.get(i);
        if (file.filename == filename)
            return file.file_inode;
    }
    return -1;
}

std::vector<std::string, malloc_allocator<std::string>> split_path(std::string str)
{
    std::vector<std::string, malloc_allocator<std::string>> ret;
    int start = str.find_first_not_of('/');
    int end = str.find('/', start);
    while (end != str.npos)
    {
        ret.push_back(str.substr(start, end - start));
        start = end + 1;
        end = str.find('/', start);
    }
    if (start < str.length())
        ret.push_back(str.substr(start));
    return ret;
}

int get_inode_from_path(std::string path)
{
    int now_inode = 0; // root
    auto filenames = split_path(path);

    for (auto &&filename : filenames)
    {
        now_inode = get_file_in_inode(now_inode, filename);
        if (now_inode == -1)
            return -1;
    }
    return now_inode;
}

//Filesystem operations that you need to implement
int fs_getattr(const char *path, struct stat *attr)
{
    Info << path;
    int now_inode = get_inode_from_path(path);
    if (now_inode == -1)
        return -ENOENT;

    auto inode = INodeProxy(now_inode).drop(); // readonly
    attr->st_mode = inode->type == INodeBlock::INodeType::DIRECTORY ? DIRMODE : REGMODE;
    attr->st_nlink = 1;
    attr->st_uid = getuid();
    attr->st_gid = getgid();
    attr->st_size = inode->filesize;
    attr->st_atime = inode->atime;
    attr->st_mtime = inode->mtime;
    attr->st_ctime = inode->ctime;

    return 0;
}

int fs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    Info << path;
    auto now_inode = fi->fh;

    DirectoryProxy directory(now_inode);
    int size = directory.length();

    for (int i = 0; i < size; i++)
    {
        auto file = directory.get(i);
        Debug << Show(file.filename);
        filler(buffer, file.filename, NULL, 0);
    }

    return 0;
}

int fs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi)
{
    Debug << path << Show(size) << Show(offset);
    auto file_inode = fi->fh;
    DataProxy data(file_inode);
    return data.read(offset, size, buffer);
}

int make_node(const char *path, INodeBlock::INodeType mode)
{
    Info << path << Show((int)mode);
    std::string strpath = path;
    std::string dirname = strpath.substr(0, strpath.find_last_of('/'));
    std::string filename = strpath.substr(strpath.find_last_of('/') + 1);

    int dirnode = get_inode_from_path(dirname);

    if (dirnode == -1)
        return -ENOENT;
    int filenode = get_file_in_inode(dirnode, filename);
    DirectoryProxy directory(dirnode);
    if (filenode == -1)
    {
        // Create new
        if (filename.length() > 24)
            return -ENOSPC;
        DirectoryProxy::Item item;
        strcpy(item.filename, filename.c_str());
        item.file_inode = filenode = Disk::alloc_inode();
        if (item.file_inode == -1)
            return -ENOSPC;
        {
            INodeProxy inode(item.file_inode);
            memset(&*inode, 0, sizeof(decltype(*inode)));
            inode->ctime = inode->atime = inode->mtime = time(NULL);
            inode->type = mode;
            inode.commit();
        }
        if (auto err = directory.push(item))
        {
            Disk::free_inode(item.file_inode);
            return -err;
        }
    }

    return 0;
}

int delete_node(const char *path)
{
    Info << path;
    std::string strpath = path;
    std::string dirname = strpath.substr(0, strpath.find_last_of('/'));
    std::string filename = strpath.substr(strpath.find_last_of('/') + 1);

    int dirnode = get_inode_from_path(dirname);

    if (dirnode == -1)
        return -ENOENT;
    int filenode = get_file_in_inode(dirnode, filename);
    DirectoryProxy directory(dirnode);
    if (filenode == -1)
        return -ENOENT;

    // Create new
    DataProxy proxy(filenode);
    proxy.resize(0); // Delete all data

    for (int i = 0, end = directory.length(); i < end; i++)
    {
        if (directory.get(i).filename == filename)
        {
            directory.erase(i);
            Disk::free_inode(filenode);
            return 0;
        }
    }
    // This shouldn't be reached
    assert(false);

    return 0;
}

int fs_mknod(const char *path, mode_t mode, dev_t dev)
{
    return make_node(path, INodeBlock::INodeType::FILE);
}

int fs_mkdir(const char *path, mode_t mode)
{
    return make_node(path, INodeBlock::INodeType::DIRECTORY);
}

int fs_rmdir(const char *path)
{
    return delete_node(path);
}

int fs_unlink(const char *path)
{
    return delete_node(path);
}

int fs_rename(const char *oldpath, const char *newname)
{
    Info << Show(oldpath) << Show(newname);
    std::string str_oldpath = oldpath;
    std::string olddirname = str_oldpath.substr(0, str_oldpath.find_last_of('/'));
    std::string oldfilename = str_oldpath.substr(str_oldpath.find_last_of('/') + 1);

    std::string str_newpath = newname;
    std::string newdirname = str_newpath.substr(0, str_newpath.find_last_of('/'));
    std::string newfilename = str_newpath.substr(str_newpath.find_last_of('/') + 1);

    if (newfilename.length() > 24)
        return -ENOSPC;
    if (get_inode_from_path(oldpath) == -1)
        return -ENOENT;
    if (get_inode_from_path(newname) != -1)
        return -EACCES;

    if (olddirname == newdirname)
    {
        int dirnode = get_inode_from_path(olddirname);
        if (dirnode == -1)
            return -ENOENT;
        DirectoryProxy directory(dirnode);

        for (int i = 0, end = directory.length(); i < end; i++)
        {
            auto path = directory.get(i);
            if (path.filename == oldfilename)
            {
                strcpy(path.filename, newfilename.c_str());
                directory.set(i, path);
                return 0;
            }
        }
        // This should not be reached
        assert(false);
    }
    else
    {
        int olddirnode = get_inode_from_path(olddirname);
        int newdirnode = get_inode_from_path(newdirname);
        if (olddirnode == -1 || newdirnode == -1)
            return -ENOENT;
        DirectoryProxy old_(olddirnode), new_(newdirnode);

        for (int i = 0, end = old_.length(); i < end; i++)
        {
            auto path = old_.get(i);
            if (path.filename == oldfilename)
            {
                strcpy(path.filename, newfilename.c_str());
                if (auto err = new_.push(path))
                    return -err;
                old_.erase(i);
                return 0;
            }
        }
        // This should not be reached
        assert(false);
    }
    return 0;
}

int fs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi)
{
    Debug << path << Show(size) << Show(offset);
    auto file_inode = fi->fh;
    auto inode = INodeProxy(file_inode).drop();
    DataProxy data(file_inode);
    if (size + offset > inode->filesize)
        if (auto err = data.resize(size + offset))
            return -err;
    return data.write(offset, size, buffer);
}

int fs_truncate(const char *path, off_t size)
{
    Info << path << Show(size);
    auto now_inode = get_inode_from_path(path);

    if (now_inode == -1)
        return -ENOENT;

    return -DataProxy(now_inode).resize(size);
}

int fs_utime(const char *path, struct utimbuf *buffer)
{
    Info;
    auto inodenum = get_inode_from_path(path);
    if (inodenum == -1)
        return -ENOENT;
    auto inode = INodeProxy(inodenum);
    inode->mtime = buffer->modtime;
    inode->atime = buffer->actime;
    inode->ctime = time(NULL);
    inode.commit();

    return 0;
}

int fs_statfs(const char *path, struct statvfs *stat)
{
    Info;
    auto header = Disk::get_header().drop(); // read only
    stat->f_bsize = BLOCK_SIZE;
    stat->f_blocks = header->data_block_num_tot;
    stat->f_bfree = stat->f_bavail = header->data_block_num_free;
    stat->f_files = header->inode_num_tot;
    stat->f_ffree = stat->f_favail = header->inode_num_free;
    return 0;
}

int fs_open(const char *path, struct fuse_file_info *fi)
{
    Info << path;

    int now_inode = 0; // root
    auto filenames = split_path(path);

    for (auto &&filename : filenames)
    {
        now_inode = get_file_in_inode(now_inode, filename);
        if (now_inode == -1)
            return -ENOENT;
    }

    fi->fh = now_inode;
    return 0;
}

//Functions you don't actually need to modify
int fs_release(const char *path, struct fuse_file_info *fi)
{
    Info;
    return 0;
}

int fs_opendir(const char *path, struct fuse_file_info *fi)
{
    Info << path;

    int now_inode = 0; // root
    auto filenames = split_path(path);

    for (auto &&filename : filenames)
    {
        now_inode = get_file_in_inode(now_inode, filename);
        if (now_inode == -1)
            return -ENOENT;
    }

    fi->fh = now_inode;
    return 0;
}

int fs_releasedir(const char *path, struct fuse_file_info *fi)
{
    Info;
    return 0;
}

static struct fuse_operations fs_operations = {};

int main(int argc, char *argv[])
{
    if (disk_init())
    {
        printf("Can't open virtual disk!\n");
        return -1;
    }
    if (mkfs())
    {
        printf("Mkfs failed!\n");
        return -2;
    }

    fs_operations.getattr = fs_getattr,
    fs_operations.mknod = fs_mknod,
    fs_operations.mkdir = fs_mkdir,
    fs_operations.unlink = fs_unlink,
    fs_operations.rmdir = fs_rmdir,
    fs_operations.rename = fs_rename,
    fs_operations.truncate = fs_truncate,
    fs_operations.utime = fs_utime,
    fs_operations.open = fs_open,
    fs_operations.read = fs_read,
    fs_operations.write = fs_write,
    fs_operations.statfs = fs_statfs,
    fs_operations.release = fs_release,
    fs_operations.opendir = fs_opendir,
    fs_operations.readdir = fs_readdir,
    fs_operations.releasedir = fs_releasedir;

    return fuse_main(argc, argv, &fs_operations, NULL);
}
void *operator new(unsigned long x)
{
    return malloc(x);
}
void operator delete(void *p)
{
    free(p);
}
void operator delete(void *p, std::size_t)
{
    free(p);
}

namespace std
{
    void __throw_length_error(char const *c)
    {
        Error << c;
        assert(false);
    }
    void __throw_logic_error(char const *c)
    {
        Error << c;
        assert(false);
    }
    void __throw_out_of_range_fmt(char const *c, ...)
    {
        Error << c;
        assert(false);
    }
} // namespace std
