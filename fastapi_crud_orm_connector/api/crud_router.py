from collections import defaultdict
from typing import List, Dict, Callable

from fastapi import Request, Depends, Response, APIRouter, Query

from fastapi_crud_orm_connector.api.query_parser import json_parser
from fastapi_crud_orm_connector.orm.crud import DataSort, DataSortType, MetadataTreeRequest, MetadataRequest, Crud


class DefaultAdminRouter:
    def __init__(self, crud: Crud):
        self.crud = crud

    def get_all(self, get_db=None, convert2schema=True) -> Callable:
        async def call(request: Request,
                       response: Response,
                       data_filter=Depends(json_parser(Query('{}', alias='filter'), return_type=Dict)),
                       data_range=Depends(json_parser(Query('[]', alias='range'), return_type=List, default=[0, 100])),
                       data_sort=Depends(json_parser(Query('[]', alias='sort'), return_type=List)),
                       data_fields=Depends(json_parser(Query('[]', alias='fields'), return_type=List)),
                       db=Depends(get_db),
                       ):
            self.crud.use_db(db)
            params = dict()
            if data_sort and data_sort[0]:
                params['data_sort'] = DataSort(field=data_sort[0], type=DataSortType[data_sort[1]])
            params['limit'] = limit = data_range[1] - data_range[0] + 1
            params['offset'] = offset = data_range[0]
            get_all_response = self.crud.get_all(data_filter=data_filter, **params, data_fields=data_fields, convert2schema=convert2schema)

            # This is necessary for react-admin to work
            response.headers["Content-Range"] = f"{offset}-{offset + limit}/{get_all_response.count}"

            return get_all_response.list

        return call

    def details(self, get_db=None) -> Callable:
        async def call(request: Request, id: int, db=Depends(get_db)):
            self.crud.use_db(db)
            return self.crud.get(id)

        return call

    def create(self, get_db=None):
        async def call(request: Request, generic, db=Depends(get_db)):
            self.crud.use_db(db)
            return self.crud.create(generic)

        return call

    def edit(self, get_db=None):
        async def call(request: Request, id: int, generic, db=Depends(get_db)):
            self.crud.use_db(db)
            return self.crud.edit(id, generic)

        return call

    def delete(self, get_db=None):
        async def call(request: Request, id: int, db=Depends(get_db)):
            self.crud.use_db(db)
            self.crud.delete(id)
            return dict()

        return call


class MetadataAdminRouter(DefaultAdminRouter):
    def __init__(self, crud: Crud, metadata_crud):
        super().__init__(crud)
        self.metadata_crud = metadata_crud

    def metadata_get_all(self, get_db=None):
        async def call(request: Request,
                       response: Response,
                       metadata_request: MetadataRequest,
                       db=Depends(get_db)):
            self.crud.use_db(db)
            metadata = self.metadata_crud.get_all(
                data_filter=dict(id=metadata_request.map_fields) if metadata_request.map_fields else None,
                data_parse={"path": lambda v: v.str.split('>>')},
                convert2schema=False).list
            metadata['name'] = metadata['id']
            metadata = metadata.set_index('id').T.to_dict()
            return metadata

        return call

    def tree(self, get_db=None):
        async def call(request: Request,
                       response: Response,
                       metadata_request: MetadataTreeRequest,
                       db=Depends(get_db)):
            self.crud.use_db(db)
            return self.metadata_crud.generate_tree(metadata_request)

        return call


def configure_crud_router(
        r: APIRouter,
        url: str,
        get_db=None,
        router: DefaultAdminRouter = None,
        arg_map=None,
        include_response_model=True,
):
    if arg_map is None:
        arg_map = dict()
    _arg_map = defaultdict(dict)
    _arg_map.update(arg_map)

    crud = router.crud

    r.get(url,
          response_model=List[crud.schema.instance] if include_response_model else None,
          response_model_exclude_none=True,
          **_arg_map['get_all'])(router.get_all(get_db))
    r.get(url + "/{id}",
          response_model=crud.schema.instance if include_response_model else None,
          response_model_exclude_none=True,
          **_arg_map['details'])(router.details(get_db))
    r.post(url,
           response_model=crud.schema.instance if include_response_model else None,
           response_model_exclude_none=True,
           **_arg_map['create'])(router.create(get_db))
    r.put(url + "/{id}",
          response_model=crud.schema.instance if include_response_model else None,
          response_model_exclude_none=True,
          **_arg_map['edit'])(router.edit(get_db))
    r.delete(url + "/{id}", response_model_exclude_none=True, **_arg_map['delete'])(router.delete(get_db))

    if isinstance(router, MetadataAdminRouter) is not None:
        r.post(url + '/metadata', response_model=Dict if include_response_model else None,
               **_arg_map['metadata_get_all'])(router.metadata_get_all(get_db))
        r.post(url + '/metadata/tree', **_arg_map['tree'])(router.tree(get_db))

    return r
