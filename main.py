import chardet
import os
import time
from github import Github
from elasticsearch import Elasticsearch

# 连接elasticsearch
es = Elasticsearch(hosts=["localhost:9200"])
# 需要同步的文件夹路径
folder_path = os.path.join(os.getcwd(), "files")
# GitHub仓库的地址和文件夹名称
github_repo = "username/repo"
github_folder = "folder"
# GitHub个人访问令牌，需要授权PyGithub库使用
github_token = "YOUR_GITHUB_TOKEN"

# 从GitHub上拉取指定的仓库和文件夹，将其下载到本地指定的文件夹中
def pull_github_folder(github_repo, github_folder, folder_path):
    # 创建GitHub API的连接
    g = Github(github_token)
    # 获取指定的仓库和文件夹
    repo = g.get_repo(github_repo)
    contents = repo.get_contents(github_folder)
    # 遍历文件夹中的所有文件，并将其下载到本地指定的文件夹中
    for content in contents:
        file_path = os.path.join(folder_path, content.name)
        if content.encoding is None:
            continue
        with open(file_path, "wb") as f:
            f.write(content.decoded_content)


# 扫描文件夹中的文件，返回文件名和内容的字典
def scan_folder(folder_path):
    files = {}
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        # 如果是文件而不是文件夹，则将其添加到字典中
        if os.path.isfile(file_path):
            # 使用chardet库检测文件编码方式
            with open(file_path, "rb") as f:
                content = f.read()
                encoding = chardet.detect(content)["encoding"]
                if encoding is None:
                    encoding = "utf-8"
            with open(file_path, "r", encoding=encoding) as f:
                # 将文件内容读取出来，作为字典的value
                file_content = f.read()
                files[filename] = file_content
    return files


# 同步文件到elasticsearch中
def sync_files_to_es(folder_path):
    # 从GitHub上拉取最新的文件夹到本地
    pull_github_folder(github_repo, github_folder, folder_path)
    # 获取文件夹中的所有文件
    files = scan_folder(folder_path)
    # 遍历所有文件
    for filename, content in files.items():
        # 构建elasticsearch的索引名和类型名
        index_name = "blog"
        # 构建elasticsearch的文档ID，这里使用文件名作为ID
        doc_id = filename
        # 构建elasticsearch的文档内容
        doc_body = {"filename": filename, "content": content}
        # 判断该文件是否已经存在于elasticsearch中
        if es.exists(index=index_name, id=doc_id):
            # 如果已经存在，则使用update方法进行更新
            es.update(index=index_name, id=doc_id, body={"doc": doc_body})
        else:
            # 如果不存在，则使用index方法进行插入
            es.index(index=index_name, id=doc_id, body=doc_body)
    # 获取elasticsearch中已有的所有文件
    es_files = es.search(index=index_name, body={"query": {"match_all": {}}})
    es_filenames = set([hit["_id"] for hit in es_files["hits"]["hits"]])
    # 遍历elasticsearch中的所有文件
    for filename in es_filenames:
        # 如果文件已经被删除，则从elasticsearch中删除该文件
        if filename not in files:
            es.delete(index=index_name, id=filename)


# 定时执行同步操作
while True:
    sync_files_to_es(folder_path)
    # 每隔一段时间执行一次同步操作，这里设置为1小时
    time.sleep(60 * 60)
