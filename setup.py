from setuptools import setup

setup(
    name = 's3client',
    packages = ['s3client'],
    version = '0.0.2',
    description = 'Handling s3 folders and files as if those are local file by using os and os.path like functions.',
    author = 'Junya Kaneko',
    author_email = 'jyuneko@hotmail.com',
    url = 'https://github.com/JunyaKaneko/pys3client',
    download_url = 'https://github.com/JunyaKaneko/pys3client.git',
    keywords = ['s3', ],
    license='MIT',
    install_requires = ['boto3', 'toml'],
    classifiers = [],
)
