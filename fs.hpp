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

#include <new>

#include <numeric>
#include <vector>
#include <algorithm>

#include <execinfo.h>

#ifdef DEBUG
#include <functional>
#include <iostream>
#include <string>
#include <sstream>
#endif

#ifndef LOG_LEVEL
#define LOG_LEVEL LEVEL_INFO
#endif

#ifdef assert
#undef assert
#define assert(expr)                          \
    if (!(expr))                              \
    {                                         \
        Error << "Assertion fail" << #expr;   \
        void *array[10];                      \
        size_t size;                          \
                                              \
        size = backtrace(array, 10);          \
        backtrace_symbols_fd(array, size, 2); \
        exit(-1);                             \
    }
#endif

enum
{
    LEVEL_DEBUG,
    LEVEL_INFO,
    LEVEL_ERROR
};

class
{
public:
    class Proxy
    {
        bool dummy;
        bool newline;

#ifdef DEBUG
        std::stringstream ss;
#endif

    public:
        Proxy(bool dummy) : newline(true), dummy(dummy) {}
        Proxy(Proxy &&r)
        {
            dummy = r.dummy;
            newline = r.newline;
            r.newline = false;
        }
        ~Proxy()
        {
#ifdef DEBUG
            std::cerr << ss.str();
            if (!dummy && newline)
                std::cerr << std::endl;
            std::cerr.flush();
#endif
        }
        template <typename T>
        Proxy &operator<<(const T &r)
        {
#ifdef DEBUG
            if (!dummy)
                ss << r << " ";
#endif
            return *this;
        }
    };
    Proxy operator()(int level)
    {
        if (level < LOG_LEVEL)
            return Proxy(true);
        else
            return Proxy(false);
    }
} inline Logger;

#define Debug Logger(LEVEL_DEBUG) << __PRETTY_FUNCTION__ << " "
#define Info Logger(LEVEL_INFO) << __PRETTY_FUNCTION__ << " "
#define Error Logger(LEVEL_ERROR) << __PRETTY_FUNCTION__ << " "
#define Show(x) (#x) << "=" << (x)

template <typename Function>
class Defer
{
private:
    bool closed;
    Function function;

public:
    Defer(Function function)
        : function(function), closed(false)
    {
    }
    ~Defer()
    {
        if (!closed)
            function();
    }
    void commit()
    {
        closed = true;
    }
};

template <class T>
class malloc_allocator
{
public:
    typedef T value_type;
    typedef value_type *pointer;
    typedef const value_type *const_pointer;
    typedef value_type &reference;
    typedef const value_type &const_reference;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    pointer address(reference x) const { return &x; }
    const_pointer address(const_reference x) const
    {
        return &x;
    }
    pointer allocate(size_type n, const_pointer = 0)
    {
        void *p = malloc(n * sizeof(T));
        return static_cast<pointer>(p);
    }
    void deallocate(pointer p, size_type)
    {
        free(p);
    }
    size_type max_size() const
    {
        return static_cast<size_type>(-1) / sizeof(value_type);
    }
    void construct(pointer p, const value_type &x)
    {
        new (p) value_type(x);
    }
    void destroy(pointer p) { p->~value_type(); }
};

template <class BlockType>
class BlockProxy
{
public:
    struct value_type
    {
        BlockType data;
        uint8_t padding[BLOCK_SIZE - sizeof(BlockType)];
    };

private:
    bool closed;
    bool error;
    int blockno;
    typename std::aligned_storage<sizeof(value_type), 8>::type data_storage;

public:
    value_type &block;

    BlockProxy() : error(1), closed(1), blockno(-1), block(reinterpret_cast<value_type &>(data_storage)) {}

    BlockProxy(int blockno);

    ~BlockProxy()
    {
        if (!closed)
            Error << "Unexpected destruct";
    }

    BlockType &operator*()
    {
        return block.data;
    }

    const BlockType &operator*() const
    {
        return block.data;
    }

    BlockType *operator->()
    {
        return &block.data;
    }

    const BlockType *operator->() const
    {
        return &block.data;
    }

    operator bool() const
    {
        return error;
    }

    void apply();

    auto &&drop()
    {
        closed = true;
        return *this;
    }

    void commit()
    {
        apply();
        if (!error)
            closed = true;
    }

    int getBlockNo() const
    {
        return blockno;
    }

    void setBlockNo(int blockno)
    {
        closed = false;
        BlockProxy::blockno = blockno;
    }
};

struct INodeBlock
{
public:
    inline static constexpr int bias = 256;
    enum class INodeType
    {
        FILE,
        DIRECTORY
    };
    struct INode
    {
        INodeType type;
        uint32_t filesize;
        uint32_t atime;
        uint32_t mtime;
        uint32_t ctime;
        uint32_t direct_pointer;
        uint32_t indirect_pointer;
        uint32_t iindirect_pointer;
    };
    static inline constexpr uint32_t INODE_IN_BLOCK = BLOCK_SIZE / sizeof(INode);

    INode inodes[INODE_IN_BLOCK];
};

struct BitmapBlock
{
public:
    inline static constexpr int bias = 512;
    using value_type = uint64_t;
    value_type data[BLOCK_SIZE / sizeof(value_type)];

    std::pair<int, int> unpack(int pos)
    {
        return {pos / (sizeof(value_type) * 8), pos % (sizeof(value_type) * 8)};
    }

    int pack(int block, int offset)
    {
        return block * (sizeof(value_type) * 8) + offset;
    }

    void set(int pos)
    {
        auto [block, offset] = unpack(pos);
        Debug << Show(block) << Show(offset);
        data[block] = data[block] | (value_type(1) << offset);
    }

    void clear(int pos)
    {
        auto [block, offset] = unpack(pos);
        Debug << Show(block) << Show(offset);
        data[block] = data[block] & ~(value_type(1) << offset);
    }

    bool get(int pos)
    {
        auto [block, offset] = unpack(pos);
        bool ret = data[block] & (value_type(1) << offset);
        Debug << Show(block) << Show(offset) << Show(ret);
        return ret;
    }

    int get_first_zero(int pos = 0)
    {
        for (int block = unpack(pos).first; block < BLOCK_SIZE / sizeof(value_type); block++)
        {
            auto &&now = data[block];
            if (~now)
            {
                int offset = __builtin_ffsll(~now) - 1;
                Debug << Show(now) << Show(block) << Show(offset);
                return pack(block, offset);
            }
        }
        return -1;
    }
};

struct BitMap
{
    static inline constexpr uint32_t siz = BLOCK_SIZE * 8;
    int minpos;
    int start, end, size;
    BitMap(int start, int end, int minpos = 0) : start(start), end(end), size((end - start) * siz), minpos(minpos) {}

    std::pair<int, int> unpack(int pos)
    {
        return {pos / siz, pos % siz};
    }

    int pack(int block, int offset)
    {
        return block * siz + offset;
    }

    void set(int pos);

    void clear(int pos);

    bool get(int pos);

    int get_first_zero();
};

struct PointerBlock
{
public:
    inline static constexpr int bias = 256;
    inline static constexpr int POINTER_PER_BLOCK = BLOCK_SIZE / sizeof(uint32_t);
    uint32_t pointers[POINTER_PER_BLOCK];
};

struct DataBlock
{
    inline static constexpr int bias = 0;
    char data[BLOCK_SIZE];
};

struct DirectoryProxy
{
public:
    int inodeno;
    struct Item
    {
        uint32_t file_inode;
        char filename[28];
    };
    static_assert(sizeof(DirectoryProxy::Item) == 32);

    DirectoryProxy(int inodeno) : inodeno(inodeno) {}

    int length();
    void set(int index, DirectoryProxy::Item item);
    DirectoryProxy::Item get(int index);
    int push(DirectoryProxy::Item file);
    void erase(int index);
};

struct HeaderBlock
{
public:
    inline static constexpr int bias = 1024;
    static inline constexpr uint32_t MAGIC_NUMBER_VAL = 0x19260817;
    uint32_t MAGIC_NUMBER;
    uint32_t inode_num_tot;
    uint32_t inode_num_free;
    uint32_t inode_bitmap_offset; // in block
    uint32_t inode_block_offset;  // in block
    uint32_t data_block_num_tot;
    uint32_t data_block_num_free;
    uint32_t data_block_bitmap_offset; // in block
    uint32_t data_block_offset;        // in block

    HeaderBlock()
        : MAGIC_NUMBER(MAGIC_NUMBER_VAL)
    {
#define DIVIDE_CEIL(x, y) (((x) + (y)-1) / (y))
        uint32_t avail_block_num = BLOCK_NUM - 1;
        uint32_t inode_blocks = avail_block_num / INodeBlock::INODE_IN_BLOCK;
        inode_num_free = inode_num_tot = inode_blocks * INodeBlock::INODE_IN_BLOCK;
        uint32_t inode_bitmap_blocks = DIVIDE_CEIL(inode_num_tot, BLOCK_SIZE * 8);
        inode_bitmap_offset = 1;
        data_block_bitmap_offset = inode_bitmap_offset + inode_bitmap_blocks;
        avail_block_num -= inode_blocks + inode_bitmap_blocks;
        uint32_t data_block_bitmap_blocks = DIVIDE_CEIL(avail_block_num, BLOCK_SIZE * 8);
        data_block_num_free = data_block_num_tot = avail_block_num - data_block_bitmap_blocks;
        inode_block_offset = data_block_bitmap_offset + data_block_bitmap_blocks;
        data_block_offset = inode_block_offset + inode_blocks;

#undef DIVIDE_CEIL
    }
};

struct INodeProxy
{
    bool closed;
    bool error;
    int inodeno;
    INodeBlock::INode inode;

    INodeProxy(int inodeno);
    ~INodeProxy()
    {
        if (!closed)
            Info << "Unexpected destruct";
    }

    INodeBlock::INode &operator*()
    {
        return inode;
    }

    INodeBlock::INode *operator->()
    {
        return &inode;
    }

    operator bool()
    {
        return error;
    }

    void apply();
    void commit()
    {
        apply();
        if (!error)
            closed = true;
    }
    auto &&drop()
    {
        closed = true;
        return *this;
    }
};

struct DataProxy
{
    int pos;
    int inodeno;
    DataProxy(int inodeno) : pos(0), inodeno(inodeno) {}

    int get_block_size();
    int resize(size_t size);
    int get_data_block(int datano);
    size_t read(size_t offset, size_t length, void *data);
    size_t write(size_t offset, size_t length, const void *data);
};

class Disk
{
    static inline constexpr int CACHE_INITIAL_SIZE = 64;
    struct CacheBlock
    {
        int blockno;
        uint32_t timestamp;
        char data[BLOCK_SIZE];
        CacheBlock() : blockno(-1), timestamp(0x3FFFFFFF) {}
    };

    template <int bias>
    static int __read(int blockno, void *buffer)
    {
        return disk_read(blockno, buffer);
    }

    template <int bias>
    static int __write(int blockno, void *buffer)
    {
        return disk_write(blockno, buffer);
    }

    inline static int data_bitmap_min_pos = 0, inode_bitmap_min_pos = 0;

public:
    static void __flush()
    {
        Info;
    }
    template <int bias, typename BlockType>
    static bool read(int blockno, BlockType &block)
    {
        static_assert(sizeof(BlockType) == BLOCK_SIZE);
        return __read<bias>(blockno, &block);
    }

    template <int bias, typename BlockType>
    static int write(int blockno, const BlockType &block)
    {
        static_assert(sizeof(BlockType) == BLOCK_SIZE);
        return __write<bias>(blockno, const_cast<BlockType *>(&block));
    }

    template <typename BlockType>
    static BlockProxy<BlockType> from_blockno(int blockno)
    {
        return BlockProxy<BlockType>(blockno);
    }

    static auto get_header()
    {
        return BlockProxy<HeaderBlock>(0);
    }

    static int alloc_inode()
    {
        auto header = get_header();
        if (header->inode_num_free == 0)
        {
            header.drop();
            return -1;
        }
        auto bitmap = BitMap(header->inode_bitmap_offset, header->data_block_bitmap_offset, inode_bitmap_min_pos);
        auto ret = bitmap.get_first_zero();

        // This should never happen
        assert(ret != -1);

        bitmap.set(ret);
        header->inode_num_free -= 1;
        header.commit();

        inode_bitmap_min_pos = std::max(inode_bitmap_min_pos, ret);

        Info << Show(ret) << Show(inode_bitmap_min_pos);

        return ret;
    }

    static void free_inode(int inodeno)
    {
        Info << Show(inodeno);
        auto header = get_header();
        auto bitmap = BitMap(header->inode_bitmap_offset, header->data_block_bitmap_offset);
        assert(bitmap.get(inodeno));
        bitmap.clear(inodeno);
        header->inode_num_free += 1;
        header.commit();

        inode_bitmap_min_pos = std::min(inode_bitmap_min_pos, inodeno);
    }

    static int alloc_data()
    {
        auto header = get_header();
        if (header->data_block_num_free == 0)
        {
            header.drop();
            return -1;
        }
        auto bitmap = BitMap(header->data_block_bitmap_offset, header->inode_block_offset, data_bitmap_min_pos);
        auto ret = bitmap.get_first_zero();

        // This should never happen
        assert(ret != -1);

        data_bitmap_min_pos = std::max(data_bitmap_min_pos, ret);

        bitmap.set(ret);
        header->data_block_num_free -= 1;
        ret += header->data_block_offset;
        header.commit();

        Debug << Show(ret) << Show(data_bitmap_min_pos);

        return ret;
    }

    static void free_data(int datano)
    {
        Debug << Show(datano);
        auto header = get_header();
        datano -= header->data_block_offset;
        assert(datano >= 0);
        auto bitmap = BitMap(header->data_block_bitmap_offset, header->inode_block_offset);
        assert(bitmap.get(datano));
        bitmap.clear(datano);
        header->data_block_num_free += 1;
        header.commit();

        data_bitmap_min_pos = std::min(data_bitmap_min_pos, datano);
    }

    static int mkfs()
    {
        Info;
        atexit(&__flush);
        // Initialize metadata
        {
            // Initialize header
            auto header = from_blockno<HeaderBlock>(0);
            new (&*header) HeaderBlock;
            header.commit();
            {
                // Initialize bitmap
                auto bitmap = from_blockno<BitmapBlock>(1);
                memset(&*bitmap, 0, BLOCK_SIZE);

                for (int i = 1; i < header->inode_block_offset; i++)
                {
                    bitmap.setBlockNo(i);
                    bitmap.commit();
                }
            }
        }
        // Initialize root

        int rootINodeNo = alloc_inode();
        auto rootINode = INodeProxy(rootINodeNo);
        rootINode->atime = rootINode->ctime = rootINode->mtime = time(NULL);
        rootINode->filesize = 0;
        rootINode->type = INodeBlock::INodeType::DIRECTORY;
        rootINode->direct_pointer = rootINode->indirect_pointer = rootINode->iindirect_pointer = 0;
        rootINode.commit();

        DataProxy root(rootINodeNo);
        root.resize(0);

        return 0;
    }
};

template <typename T>
BlockProxy<T>::BlockProxy(int blockno)
    : closed(false), error(false), blockno(blockno), block(*reinterpret_cast<value_type *>(&data_storage))
{
    if (!(blockno >= 0 && blockno < BLOCK_NUM))
    {
        Error << Show(blockno);
        assert(blockno >= 0 && blockno < BLOCK_NUM);
    }
    error = Disk::read<T::bias>(blockno, block);

    Debug << Show(blockno) << Show(error);
}

template <typename T>
void BlockProxy<T>::apply()
{
    assert(!closed);
    error = Disk::write<T::bias>(blockno, block);
    Debug << Show(blockno) << Show(error);
}

void BitMap::set(int pos)
{
    Debug << Show(pos);
    auto [blockno, offset] = unpack(pos);
    auto block = Disk::from_blockno<BitmapBlock>(blockno + start);
    block->set(offset);
    block.commit();
}

void BitMap::clear(int pos)
{
    Debug << Show(pos);
    auto [blockno, offset] = unpack(pos);
    auto block = Disk::from_blockno<BitmapBlock>(blockno + start);
    block->clear(offset);
    block.commit();
}

bool BitMap::get(int pos)
{
    auto [blockno, offset] = unpack(pos);
    auto block = Disk::from_blockno<BitmapBlock>(blockno + start).drop();
    auto ret = block->get(offset);
    Debug << Show(pos) << Show(ret);
    return ret;
}

int BitMap::get_first_zero()
{
    auto ret = -1;
    auto [blockoff, off] = unpack(minpos);
    for (int pos = start + blockoff, blockno = blockoff; pos < end; pos++, blockno++)
    {
        auto block = Disk::from_blockno<BitmapBlock>(pos);
        int _ = block->get_first_zero(off);
        off = 0;
        block.drop();
        if (_ != -1)
        {
            ret = _ + blockno * siz;
            break;
        }
    }
    Debug << Show(ret);
    return ret;
}

INodeProxy::INodeProxy(int inodeno)
    : inodeno(inodeno)
{
    auto header = Disk::get_header();
    header.drop();
    int blockno = inodeno / INodeBlock::INODE_IN_BLOCK + header->inode_block_offset;
    int offset = inodeno % INodeBlock::INODE_IN_BLOCK;

    auto block = Disk::from_blockno<INodeBlock>(blockno);
    error = block;
    inode = block->inodes[offset];
    block.drop();
    Debug << Show(inodeno) << Show(error);
}

void INodeProxy::apply()
{
    auto header = Disk::get_header();
    header.drop();
    int blockno = inodeno / INodeBlock::INODE_IN_BLOCK + header->inode_block_offset;
    int offset = inodeno % INodeBlock::INODE_IN_BLOCK;

    auto block = Disk::from_blockno<INodeBlock>(blockno);

    if (block)
    {
        error = true;
        return;
    }

    block->inodes[offset] = inode;
    block.commit();
    error = block;
    Debug << Show(inodeno) << Show(error);
}

inline int DataProxy::get_block_size()
{
    auto inode = INodeProxy(inodeno).drop(); //read only
    auto pointers = BlockProxy<PointerBlock>();
    size_t now_block = 0;

    if (inode->direct_pointer == 0)
        goto BLOCK_COUNT_END;

    now_block++;

    if (inode->indirect_pointer == 0)
        goto BLOCK_COUNT_END;

    pointers.~BlockProxy();
    new (&pointers) BlockProxy<PointerBlock>(inode->indirect_pointer);
    pointers.drop();

    for (auto &&pointer : pointers->pointers)
    {
        if (pointer == 0)
            goto BLOCK_COUNT_END;
        now_block++;
    }

    if (inode->iindirect_pointer == 0)
        goto BLOCK_COUNT_END;

    pointers.~BlockProxy();
    new (&pointers) BlockProxy<PointerBlock>(inode->iindirect_pointer);
    pointers.drop();

    for (int now = 0; now < PointerBlock::POINTER_PER_BLOCK; now++)
    {
        if (pointers->pointers[now] == 0)
        {
            if (now != 0)
            {
                // Check prev block;
                now--;
                now_block -= PointerBlock::POINTER_PER_BLOCK;
                auto block_to_check = pointers->pointers[now];
                pointers.~BlockProxy();
                new (&pointers) BlockProxy<PointerBlock>(block_to_check);
                pointers.drop();
                for (auto &&pointer : pointers->pointers)
                {
                    if (pointer == 0)
                        break;
                    now_block++;
                }
            }
            goto BLOCK_COUNT_END;
        }
        else
        {
            now_block += PointerBlock::POINTER_PER_BLOCK;
        }
    }
    goto BLOCK_COUNT_END;

BLOCK_COUNT_END:
    return now_block;
}

int DataProxy::resize(size_t size)
{
    int block_need = std::max<int>(0, (size + BLOCK_SIZE - 1) / BLOCK_SIZE); // ceil
    int block_now = get_block_size();

    auto inode = INodeProxy(inodeno);
    inode->ctime = time(NULL);
    // BlockProxy<PointerBlock> ind_block(-1), iind_block(-1);

    size_t size_orig = inode->filesize;

    while (block_now != block_need)
    {
        if (block_now < block_need)
        {
            if (block_now < 1)
            {
                int data = Disk::alloc_data();
                if (data == -1)
                    goto ROLLBACK;
                else
                {
                    block_now++;
                    inode->direct_pointer = data;
                }
            }
            else if (block_now < 1 + PointerBlock::POINTER_PER_BLOCK)
            {
                bool extend_ind = false;
                int offset = block_now - 1;
                if (offset == 0)
                {
                    int data = Disk::alloc_data();
                    if (data == -1)
                        goto ROLLBACK;
                    auto ind_block = Disk::from_blockno<PointerBlock>(data);
                    memset(&*ind_block, 0, BLOCK_SIZE);
                    ind_block.commit();
                    inode->indirect_pointer = data;
                    extend_ind = true;
                }
                auto ind_block = Disk::from_blockno<PointerBlock>(inode->indirect_pointer);
                int data = Disk::alloc_data();
                if (data == -1)
                {
                    if (extend_ind)
                    {
                        Disk::free_data(inode->indirect_pointer);
                        inode->indirect_pointer = 0;
                    }
                    goto ROLLBACK;
                }
                ind_block->pointers[offset] = data;
                ind_block.commit();
                block_now++;
            }
            else
            {
                bool extend_ind = false;
                bool extend_iind = false;
                int offset0 = block_now - (1 + PointerBlock::POINTER_PER_BLOCK);
                int id_ind = offset0 / PointerBlock::POINTER_PER_BLOCK;
                int id_offset = offset0 % PointerBlock::POINTER_PER_BLOCK;

                if (id_offset == 0)
                {
                    if (id_ind == 0)
                    {
                        int data = Disk::alloc_data();
                        if (data == -1)
                            goto ROLLBACK;
                        auto iind_block = Disk::from_blockno<PointerBlock>(data);
                        memset(&*iind_block, 0, BLOCK_SIZE);
                        iind_block.commit();
                        inode->iindirect_pointer = data;
                        extend_iind = true;
                    }
                    auto iind_block = Disk::from_blockno<PointerBlock>(inode->iindirect_pointer);
                    int data = Disk::alloc_data();
                    if (data == -1)
                    {
                        iind_block.drop();
                        if (extend_iind)
                        {
                            Disk::free_data(inode->iindirect_pointer);
                            inode->iindirect_pointer = 0;
                        }
                        goto ROLLBACK;
                    }
                    auto ind_block = Disk::from_blockno<PointerBlock>(data);
                    memset(&*ind_block, 0, BLOCK_SIZE);
                    ind_block.commit();
                    iind_block->pointers[id_ind] = data;
                    iind_block.commit();
                    extend_ind = true;
                }

                auto iind_block = Disk::from_blockno<PointerBlock>(inode->iindirect_pointer);
                auto ind_block = Disk::from_blockno<PointerBlock>(iind_block->pointers[id_ind]);

                int data = Disk::alloc_data();
                if (data == -1)
                {
                    ind_block.drop();
                    if (extend_ind)
                    {
                        Disk::free_data(iind_block->pointers[id_ind]);
                        iind_block->pointers[id_ind] = 0;
                    }
                    iind_block.commit();
                    if (extend_iind)
                    {
                        Disk::free_data(inode->iindirect_pointer);
                        inode->iindirect_pointer = 0;
                    }
                    goto ROLLBACK;
                }
                iind_block.drop();
                ind_block->pointers[id_offset] = data;
                ind_block.commit();
                block_now++;
            }
        }
        else if (block_now > block_need)
        {
            int block_to_shrink = block_now - 1;
            if (block_to_shrink < 1)
            {
                Disk::free_data(inode->direct_pointer);
                inode->direct_pointer = 0;
                block_now--;
            }
            else if (block_to_shrink < 1 + PointerBlock::POINTER_PER_BLOCK)
            {
                int offset = block_to_shrink - 1;
                auto ind_block = Disk::from_blockno<PointerBlock>(inode->indirect_pointer);
                Disk::free_data(ind_block->pointers[offset]);
                ind_block->pointers[offset] = 0;
                ind_block.commit();
                if (offset == 0)
                {
                    Disk::free_data(inode->indirect_pointer);
                    inode->indirect_pointer = 0;
                }
                block_now--;
            }
            else
            {
                int offset0 = block_to_shrink - (1 + PointerBlock::POINTER_PER_BLOCK);
                int id_ind = offset0 / PointerBlock::POINTER_PER_BLOCK;
                int id_offset = offset0 % PointerBlock::POINTER_PER_BLOCK;

                auto iind_block = Disk::from_blockno<PointerBlock>(inode->iindirect_pointer);
                auto ind_block = Disk::from_blockno<PointerBlock>(iind_block->pointers[id_ind]);
                Disk::free_data(ind_block->pointers[id_offset]);
                ind_block->pointers[id_offset] = 0;
                ind_block.commit();

                if (id_offset == 0)
                {
                    Disk::free_data(ind_block.getBlockNo());
                    iind_block->pointers[id_ind] = 0;
                    iind_block.commit();
                    if (id_ind == 0)
                    {
                        Disk::free_data(iind_block.getBlockNo());
                        inode->iindirect_pointer = 0;
                    }
                }
                else
                    iind_block.drop();
                block_now--;
            }
        }
    }
    inode->filesize = size;
    inode.commit();
    Debug << Show(inode->filesize);
    return 0;
ROLLBACK:
    Error << "Rolling back";
    inode.commit();
    resize(size_orig);
    return ENOSPC;
}

int DataProxy::get_data_block(int datano)
{
    auto inode = INodeProxy(inodeno);
    inode.drop(); //Read only

    int now_block = 0;

    if (now_block == datano)
    {
        if (inode->direct_pointer == 0)
        {
            Error << Show(inode->filesize) << Show(inodeno);
            assert(inode->direct_pointer);
        }
        return inode->direct_pointer;
    }
    now_block++;

    if (inode->indirect_pointer == 0)
    {
        Error << Show(inode->filesize) << Show(inodeno);
        assert(inode->indirect_pointer);
    }

    if (datano < now_block + PointerBlock::POINTER_PER_BLOCK)
    {
        auto pointer = Disk::from_blockno<PointerBlock>(inode->indirect_pointer).drop();
        assert(pointer->pointers[datano - now_block]);
        return pointer->pointers[datano - now_block];
    }
    now_block += PointerBlock::POINTER_PER_BLOCK;

    assert(inode->iindirect_pointer);
    auto ipointer = Disk::from_blockno<PointerBlock>(inode->iindirect_pointer).drop();
    auto offset0 = datano - now_block;
    auto id_ind = offset0 / PointerBlock::POINTER_PER_BLOCK;
    auto id_offset = offset0 % PointerBlock::POINTER_PER_BLOCK;
    assert(ipointer->pointers[id_ind]);
    auto pointer = Disk::from_blockno<PointerBlock>(ipointer->pointers[id_ind]).drop();
    assert(pointer->pointers[id_offset]);
    return pointer->pointers[id_offset];
}

size_t DataProxy::read(size_t offset, size_t size, void *target)
{
    Debug << Show(offset) << Show(size);
    auto inode = INodeProxy(inodeno);
    inode->atime = time(NULL);
    inode.commit(); // update access time

    size = std::min<size_t>(offset + size, inode->filesize) - offset;
    auto ret = size;

    while (size)
    {
        size_t datano = offset / BLOCK_SIZE;
        int block_no = get_data_block(datano);
        size_t offset_in_block = offset % BLOCK_SIZE;
        size_t bytes_to_read = std::min<size_t>(size, BLOCK_SIZE);

        auto datablock = Disk::from_blockno<DataBlock>(block_no).drop(); // read-only
        memcpy(target, datablock->data + offset_in_block, bytes_to_read);
        size -= bytes_to_read;
        offset += bytes_to_read;
        target = (char *)target + bytes_to_read;
    }

    return ret;
}

size_t DataProxy::write(size_t offset, size_t size, const void *target)
{
    Debug << Show(offset) << Show(size);
    auto inode = INodeProxy(inodeno);
    inode->mtime = time(NULL);
    inode.commit(); // update modify time

    if (offset + size > inode->filesize)
    {
        Error << Show(offset + size > inode->filesize);
        size = inode->filesize - offset;
    }
    auto ret = size;

    while (size)
    {
        size_t datano = offset / BLOCK_SIZE;
        int block_no = get_data_block(datano);
        size_t offset_in_block = offset % BLOCK_SIZE;
        size_t bytes_to_write = std::min<size_t>(size, BLOCK_SIZE);

        auto datablock = Disk::from_blockno<DataBlock>(block_no);
        memcpy(datablock->data + offset_in_block, target, bytes_to_write);
        datablock.commit();
        size -= bytes_to_write;
        offset += bytes_to_write;
        target = (char *)target + bytes_to_write;
    }

    return ret;
}

int DirectoryProxy::length() { return INodeProxy(inodeno).drop()->filesize / sizeof(Item); }

void DirectoryProxy::set(int index, DirectoryProxy::Item item)
{
    assert(index < length());
    DataProxy data(inodeno);
    data.write(index * sizeof(Item), sizeof(Item), &item);
}

DirectoryProxy::Item DirectoryProxy::get(int index)
{
    assert(index < length());
    DataProxy data(inodeno);
    Item ret;
    data.read(index * sizeof(Item), sizeof(Item), &ret);
    return ret;
}

int DirectoryProxy::push(DirectoryProxy::Item item)
{
    int length = this->length();
    int file_length = (length + 1) * sizeof(Item);
    DataProxy data(inodeno);

    auto err = data.resize(file_length);
    if (err)
        return err;
    this->set(length, item);
    return 0;
}

void DirectoryProxy::erase(int index)
{
    int length = this->length();
    auto last = this->get(length - 1);
    this->set(index, last);
    DataProxy data(inodeno);
    assert(!data.resize((length - 1) * sizeof(Item)));
}
