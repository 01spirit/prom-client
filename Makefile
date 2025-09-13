# 定义编译目标
all: query storage

# 编译 query 目录下的 query.go
query:
	cd service/query && go build -o ../../bin/query query.go

# 编译 storage 目录下的 storage.go
storage:
	cd service/storage && go build -o ../../bin/storage storage.go

# 清理编译生成的文件
clean:
	rm -f bin/query bin/storage

# 定义默认目标
.PHONY: all query storage clean