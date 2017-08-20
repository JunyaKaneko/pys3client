import unittest
import s3client


class BaseS3ClientTestCase(unittest.TestCase):
    def setUp(self):
        s3client.chbucket('s3client-test-0')

    def create_test_dir(self, f):
        s3client.chdir('/')
        s3client.mkdir(f.__name__)
        s3client.chdir(f.__name__)

    def delete_test_dir(self, f):
        s3client.chdir('/')
        s3client.removedirs(f.__name__)

    def tearDown(self):
        s3client.chdir('/')
        for dir_name in s3client.listdir():
            s3client.removedirs(dir_name)
        

def test_dir(method):
    def wrapper(self):
        self.create_test_dir(method)
        method(self)
        self.delete_test_dir(method)
    return wrapper


class MkdirTestCase(BaseS3ClientTestCase):
    @test_dir
    def test_creating_empty_dir_in_the_current_dir(self):        
        s3client.mkdir('empty_dir')
        assert s3client.path.isdir('empty_dir')

        s3client.rmdir('empty_dir')
        assert not s3client.path.exists('empty_dir')

    @test_dir
    def test_creating_dir_in_non_existing_dir(self):        
        try:
            s3client.mkdir('non_existing_dir/empty_dir')
            raise Exception('Should raise FileNotFoundError')
        except FileNotFoundError:
            pass

    @test_dir
    def test_creating_existing_dir(self):
        s3client.mkdir('existing_dir')
        try:
            s3client.mkdir('existing_dir')
            raise Exception('Should raise FileExistsError')
        except FileExistsError:
            pass
        s3client.rmdir('existing_dir')

    @test_dir
    def test_creating_dir_hierarchy(self):
        s3client.mkdir('parent_dir')
        s3client.mkdir('parent_dir/child_dir')
        s3client.path.exists('parent_dir')
        s3client.path.exists('parent_dir/child_dir')
        
        s3client.rmdir('parent_dir/child_dir')
        assert s3client.path.exists('parent_dir')
        assert not s3client.path.exists('parent_dir/child_dir')

        s3client.rmdir('parent_dir')
        assert not s3client.path.exists('parent_dir')


class RmdirTestCase(BaseS3ClientTestCase):
    @test_dir
    def test_removing_empty_dir(self):
        s3client.mkdir('empty_dir')
        s3client.rmdir('empty_dir')
        assert not s3client.path.exists('empty_dir')

    @test_dir
    def test_removing_non_empty_dir(self):
        s3client.mkdir('parent_dir')
        s3client.mkdir('parent_dir/child_dir')

        try:
            s3client.rmdir('parent_dir')
            raise Exception('Should raise S3ClientError')
        except s3client.S3ClientError:
            pass

        s3client.rmdir('parent_dir/child_dir')
        s3client.rmdir('parent_dir')

    @test_dir
    def test_removing_non_existing_dir(self):
        try:
            s3client.rmdir('non_exisitng_dir')
            raise Exception('Should raise FileNotFoundError')
        except FileNotFoundError:
            pass


class MakedirsTestCase(BaseS3ClientTestCase):
    @test_dir
    def test_creating_non_existing_dirs(self):
        s3client.makedirs('parent_dir/child_dir/grand_child_dir')
        assert s3client.path.exists('parent_dir/child_dir/grand_child_dir')
        assert s3client.path.exists('parent_dir/child_dir')
        assert s3client.path.exists('parent_dir')

        s3client.removedirs('parent_dir')
        assert not s3client.path.exists('parent_dir/child_dir/grand_child_dir')
        assert not s3client.path.exists('parent_dir/child_dir')
        assert not s3client.path.exists('parent_dir')

    @test_dir
    def test_creating_existing_dir(self):
        s3client.makedirs('parent_dir/child_dir/grand_child_dir')
        try:
            s3client.makedirs('parent_dir/child_dir')
            raise Exception('Should raise FileExistsError')
        except FileExistsError:
            pass
        s3client.removedirs('parent_dir/child_dir/grand_child_dir')


class RemovedirsTestCase(BaseS3ClientTestCase):
    @test_dir
    def test_removing_empty_dirs(self):
        s3client.makedirs('parent_dir/child_dir/grand_child_dir')
        assert s3client.path.exists('parent_dir/child_dir/grand_child_dir')
        assert s3client.path.exists('parent_dir/child_dir')
        assert s3client.path.exists('parent_dir')

        s3client.removedirs('parent_dir')
        assert not s3client.path.exists('parent_dir/child_dir/grand_child_dir')
        assert not s3client.path.exists('parent_dir/child_dir')
        assert not s3client.path.exists('parent_dir')

    @test_dir
    def test_removing_non_empty_dirs(self):
        s3client.makedirs('parent_dir/child_dir/grand_child_dir')
        s3f = s3client.file.S3File('parent_dir/child_dir/grand_child_dir/sample.txt')
        with s3f.open('w') as f:
            f.write('This is a file')
        assert s3client.path.exists('parent_dir/child_dir/grand_child_dir/sample.txt')
        assert s3client.path.exists('parent_dir/child_dir/grand_child_dir')
        assert s3client.path.exists('parent_dir/child_dir')
        assert s3client.path.exists('parent_dir')

        try:
            s3client.removedirs('parent_dir')
            raise Exception('Should raise S3ClientError')
        except s3client.S3ClientError:
            pass

        s3client.remove('parent_dir/child_dir/grand_child_dir/sample.txt')
        s3client.removedirs('parent_dir')
        assert not s3client.path.exists('parent_dir/child_dir/grand_child_dir/sample.txt')
        assert not s3client.path.exists('parent_dir/child_dir/grand_child_dir')
        assert not s3client.path.exists('parent_dir/child_dir')
        assert not s3client.path.exists('parent_dir')
        
        
class RenameTestCase(BaseS3ClientTestCase):
    @test_dir
    def test_rename_existing_file(self):
        s3f = s3client.file.S3File('sample.txt')
        with s3f.open('w') as f:
            f.write('This is a sample')
        s3client.rename('sample.txt', 'sample2.txt')
        assert not s3client.path.exists('sample.txt')
        assert s3client.path.exists('sample2.txt')

        s3client.remove('sample2.txt')
        assert not s3client.path.exists('sample2.txt')

    @test_dir
    def test_rename_non_existing_file(self):
        try:
            s3client.rename('sample.txt', 'sample1.txt')
            raise Exception('Should rise FileNotFoundError')
        except FileNotFoundError:
            pass

    @test_dir
    def test_rename_file_to_existing_dir(self):
        s3client.mkdir('parent_dir')
        s3client.mkdir('parent_dir/child_dir')
        s3f = s3client.file.S3File('parent_dir/sample.txt')
        with s3f.open('w') as f:
            f.write('This is a sample')

        try:
            s3client.rename('parent_dir/sample.txt', 'parent_dir/child_dir')
            raise Exception('Should raise S3ClientError')
        except s3client.S3ClientError:
            pass

        s3client.remove('parent_dir/sample.txt')
        s3client.removedirs('parent_dir')
        assert not s3client.path.exists('parent_dir/sample.txt')
        assert not s3client.path.exists('parent_dir/child_dir')
        assert not s3client.path.exists('parent_dir')

    @test_dir
    def test_rename_dir_to_dir(self):
        s3client.makedirs('parent_dir/child_dir')
        s3client.rename('parent_dir', 'parent_dir2')

        assert not s3client.path.exists('parent_dir.child_dir')
        assert not s3client.path.exists('parent_dir')
        assert s3client.path.exists('parent_dir2/child_dir')
        assert s3client.path.exists('parent_dir2')

        s3client.removedirs('parent_dir2')
