import json
import re
import boto3
import fitz
import uuid
from collections import namedtuple
from io import BytesIO
from typing import Callable, List
from PIL import Image
from library import S3Location


class S3InputObject:
    """
    Class for handling the uploading and splitting of a PDF document into individual pages.
    """

    # Constants for better resolution
    _ZOOM_X: float = 2.0  # horizontal zoom
    _ZOOM_Y: float = 2.0  # vertical zoom
    _OUTPUT_BUCKET: str = 'tt-assessment-bucket'
    _OUTPUT_PREFIX: str = 'in_progress'
    _MAT: fitz.Matrix = fitz.Matrix(_ZOOM_X, _ZOOM_Y)  # zoom factor 2 in each dimension

    # Named tuple for storing page information
    Page = namedtuple('Page', ['s3_document_file', 'number', 'image_obj', 'image_ext', 's3_page_file',
                               'post_processing_result'])

    def __init__(self, s3_document_file: S3Location, message_object_id: str = None,
                 output_bucket: str = _OUTPUT_BUCKET) -> None:
        """
        Initialize the class with the PDF document location and message ID.

        Args:
            s3_document_file (S3Location): S3 bucket and key for the PDF document.
            message_object_id (str): Message object ID.
            output_bucket (str): Name of the output S3 bucket.
        """
        self._s3_document_file = s3_document_file
        self._bytes_obj = None
        self._message_object_id = message_object_id
        self._output_bucket = output_bucket
        self._metadata = {'content_type': boto3.client('s3').head_object(Bucket=self._s3_document_file['S3Bucket'],
                                           Key=self._s3_document_file['S3ObjectName'])['ContentType'],
                          'document_location': s3_document_file,
                          'message_object_id': message_object_id}
        self._is_pdf = boto3.client('s3').head_object(Bucket=self._s3_document_file['S3Bucket'],
                                                      Key=self._s3_document_file['S3ObjectName'])['ContentType']\
                       == 'application/pdf'

    def _lazy_download(self, refresh: bool = False) -> None:
        """
        Lazily download the PDF document content from S3.

        Args:
            refresh (bool): Whether to refresh the content.
        """
        if refresh or self._bytes_obj is None:
            self._bytes_obj = boto3.resource('s3').Object(self._s3_document_file['S3Bucket'],
                                                          self._s3_document_file['S3ObjectName']).get()['Body'].read()

    def _save_metadata(self):
        """
        This method saves the metadata of the current object to an S3 bucket.

        The output path for the S3 bucket is obtained using the `_get_output_path` method. The metadata is then serialized
        to JSON using the `json` module and is saved as an object in the S3 bucket using the `boto3` library. The object key
        is a combination of the S3 object name and the string "metadata.json".
        """
        output_path = self._get_output_path()
        boto3.client('s3').put_object(Body=json.dumps(self._metadata), Bucket=output_path['S3Bucket'],
                                      Key='/'.join([output_path['S3ObjectName'], 'metadata.json']))

    @classmethod
    def get_output_path_from_page_image_path(cls, s3_page_image_location: S3Location, suffix: str = '') -> S3Location:
        """
        Get output path for a given S3 page image location.

        Args:
        - s3_page_image_location (S3Location): The S3 location of a page image.
        - suffix (str, optional): A suffix to add to the output path. Default is an empty string.

        Returns:
        - S3Location: The output path for the page image location.
        """
        loc_parts = s3_page_image_location['S3ObjectName'].split('/')
        base_path = '/'.join(loc_parts[:loc_parts.index('pages')])
        return cls.get_output_path(s3_page_image_location['S3Bucket'], base_path, suffix=suffix)

    @classmethod
    def get_output_path(cls, output_bucket: str, prefix: str = _OUTPUT_PREFIX, subpath: str = None,
                        suffix: str = None) -> S3Location:
        """
        Class method to get the S3 location path.

        :param output_bucket: The name of the output S3 bucket.
        :type output_bucket: str
        :param prefix: The prefix for the S3 object name. (default is _OUTPUT_PREFIX)
        :type prefix: str
        :param subpath: The subpath for the S3 object name. (default is None)
        :type subpath: str
        :param suffix: The suffix for the S3 object name. (default is None)
        :type suffix: str
        :return: A dictionary with the S3 bucket name and the S3 object name.
        :rtype: dict
        """
        return {"S3Bucket": output_bucket,
                "S3ObjectName": '/'.join([s for s in [prefix, subpath, suffix] if s is not None and s != '']).strip(
                    '/')}

    def _get_output_path(self, prefix: str = _OUTPUT_PREFIX, suffix: str = '') -> S3Location:
        """
        Instance method to get the S3 location path.

        :param prefix: The prefix for the S3 object name. (default is _OUTPUT_PREFIX)
        :type prefix: str
        :param suffix: The suffix for the S3 object name. (default is '')
        :type suffix: str
        :return: A dictionary with the S3 bucket name and the S3 object name.
        :rtype: dict
        """
        return self.get_output_path(self._output_bucket, prefix, self._message_object_id, suffix)

    def split_upload_pages(self, page_post_process: Callable = None) -> List[Page]:
        """
        Upload the splitting pages to an S3 bucket with the defined naming scheme (prefix).

        Args:
            page_post_process (Callable, optional): A callable that takes in a `S3Location` object and returns a modified
                `S3Location` object. Defaults to None.

        Returns:
            List[Page]: A list of `Page` objects, containing the uploaded splitting pages.

        """

        self._lazy_download()
        pages = []
        s3_client = boto3.client('s3')
        if self._is_pdf:
            pdf_file = fitz.open("pdf", stream=BytesIO(self._bytes_obj))
            self._metadata['page_count'] = pdf_file.page_count
            self._save_metadata()
            for page in pdf_file:
                page_type = 'other' if len(re.sub('[\W_]+', '', page.get_text())) > 5 or len(
                    page.get_images()) != 1 else 'single_image'

                if page_type == 'other':
                    image_bytes = page.get_pixmap(matrix=self._MAT).pil_tobytes('png')  # render page to an image
                    image_ext = 'png'
                else:
                    # extract the image bytes
                    base_image = pdf_file.extract_image(page.get_images()[0][0])
                    image_bytes = base_image["image"]

                    # get the image extension
                    image_ext = base_image["ext"]

                page_file: S3Location = self._get_output_path(suffix=f'pages/images/{page.number}.{image_ext}')
                buffer = BytesIO(image_bytes)
                buffer.seek(0)
                s3_client.upload_fileobj(buffer, page_file['S3Bucket'], page_file['S3ObjectName'])
                post = None
                if page_post_process is not None:
                    post = page_post_process(page_file)
                pages.append(self.Page(self._s3_document_file, page.number,
                                       Image.open(BytesIO(image_bytes)), image_ext, page_file, post))
            pdf_file.close()

        else:
            self._metadata['page_count'] = 1
            self._save_metadata()
            image_ext = self._s3_document_file['S3ObjectName'].split('.')[-1]
            page_file: S3Location = self._get_output_path(suffix=f'pages/images/0.{image_ext}')
            buffer = BytesIO(self._bytes_obj)
            buffer.seek(0)
            s3_client.upload_fileobj(buffer, page_file['S3Bucket'], page_file['S3ObjectName'])
            post = None
            if page_post_process is not None:
                post = page_post_process(page_file)
            pages.append(self.Page(self._s3_document_file, 0,
                                   Image.open(BytesIO(self._bytes_obj)), image_ext, page_file, post))
        return pages


# HINT: Keep the method lambda_handler() in mind for Assignment 2 and 3    
#def lambda_handler(event, context):
#    key = 'input/test_auftrag.pdf'
#    bucket = 'tt-assessment-bucket'
#    message_id = str(uuid.uuid1())
#    pdf_file = S3InputObject({"S3ObjectName": key, "S3Bucket": bucket}, message_id)
#    process_job = pdf_file.split_upload_pages()
    
if __name__ == '__main__':
    key = 'input/test_auftrag.pdf'
    bucket = 'tt-assessment-bucket'
    message_id = str(uuid.uuid1())
    pdf_file = S3InputObject({"S3ObjectName": key, "S3Bucket": bucket}, message_id)
    process_job = pdf_file.split_upload_pages()
