from typing import List, Dict

from fastapi import Request, Depends, Response, APIRouter, Query

from fastapi_crud_orm_connector.api.query_parser import json_parser
from fastapi_crud_orm_connector.orm.crud import Crud, DataSort, DataSortType, MetadataTreeRequest, MetadataRequest


def configure_crud_router(r: APIRouter, crud: Crud, url: str, get_db, metadata_crud=None):
    @r.get(url,
           response_model=List[crud.schema.instance],
           response_model_exclude_none=True, )
    async def get_all(request: Request,
                      response: Response,
                      data_filter=Depends(json_parser(Query('{}', alias='filter'), return_type=Dict)),
                      data_range=Depends(json_parser(Query('[]', alias='range'), return_type=List, default=[0, 100])),
                      data_sort=Depends(json_parser(Query('[]', alias='sort'), return_type=List)),
                      data_fields=Depends(json_parser(Query('[]', alias='fields'), return_type=List)),
                      db=Depends(get_db)):
        params = dict()
        if data_sort and data_sort[0]:
            params['data_sort'] = DataSort(field=data_sort[0], type=DataSortType[data_sort[1]])
        params['limit'] = limit = data_range[1] - data_range[0] + 1
        params['offset'] = offset = data_range[0]
        crud.use_db(db)
        get_all_response = crud.get_all(data_filter=data_filter, **params, data_fields=data_fields)

        # This is necessary for react-admin to work
        response.headers["Content-Range"] = f"{offset}-{offset + limit}/{get_all_response.count}"

        return get_all_response.list

    @r.get(url + "/{id}",
           response_model=crud.schema.instance,
           response_model_exclude_none=True, )
    async def details(request: Request,
                      id: int,
                      db=Depends(get_db), ):
        return crud.use_db(db).get(id)

    @r.post(url, response_model=crud.schema.instance, response_model_exclude_none=True)
    async def create(request: Request,
                     generic: crud.schema.create,
                     db=Depends(get_db), ):
        return crud.use_db(db).create(generic)

    @r.put(url + "/{id}",
           response_model=crud.schema.instance,
           response_model_exclude_none=True)
    async def edit(request: Request,
                   id: int,
                   generic: crud.schema.edit,
                   db=Depends(get_db)):
        return crud.use_db(db).edit(id, generic)

    @r.delete(url + "/{id}", response_model_exclude_none=True)
    async def delete(request: Request,
                     id: int,
                     db=Depends(get_db)):
        crud.use_db(db).delete(id)
        return dict()

    if metadata_crud:
        @r.post(url + '/metadata',
                response_model=Dict,
                response_model_exclude_none=True, )
        async def get_all(request: Request,
                          response: Response,
                          metadata_request: MetadataRequest,
                          db=Depends(get_db)):
            metadata = metadata_crud.use_db(db).get_all(data_filter=dict(id=metadata_request.map_fields) if metadata_request.map_fields else None,
                                                        data_parse={"path": lambda v: v.str.split('>>')},
                                                        to_schema=False)
            metadata['name'] = metadata['id']
            metadata = metadata.set_index('id').T.to_dict()
            return metadata

        @r.post(url + '/metadata/tree')
        async def tree(request: Request,
                       response: Response,
                       metadata_request: MetadataTreeRequest,
                       db=Depends(get_db)):
            return metadata_crud.use_db(db).generate_tree(metadata_request)

    return r
