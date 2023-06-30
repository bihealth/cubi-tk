import pathlib
from typing import List

from irods.collection import iRODSCollection
from irods.data_object import iRODSDataObject
from irods.models import Collection, DataObject


class iRODSDataObjectEq(iRODSDataObject):
    name: str
    path: str
    checksum: str

    def __eq__(self, other):
        return (
            self.name == other.name and self.path == other.path and self.checksum == other.checksum
        )


def createIrodsDataObject(
    file_name: str, irods_path: str, file_md5sum: str, replicas_md5sum: List[str]
):
    """Create iRODSDataObject from parameters."""

    parent = pathlib.Path(irods_path).parent
    collection_data = {
        Collection.id: 0,
        Collection.name: str(parent),
        Collection.create_time: None,
        Collection.modify_time: None,
        Collection.inheritance: None,
        Collection.owner_name: None,
        Collection.owner_zone: None,
    }
    collection = iRODSCollection(None, result=collection_data)

    data_object_datas = []
    for i, rep_md5sum in enumerate(replicas_md5sum):
        data_object_datas.append(
            {
                DataObject.id: 0,
                DataObject.name: file_name,
                DataObject.replica_number: i,
                DataObject.replica_status: None,
                DataObject.resource_name: None,
                DataObject.path: irods_path,
                DataObject.resc_hier: None,
                DataObject.checksum: rep_md5sum,
                DataObject.size: 0,
                DataObject.comments: "",
            }
        )
    obj = iRODSDataObjectEq(None, parent=collection, results=data_object_datas)
    return obj
