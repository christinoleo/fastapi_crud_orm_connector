from collections import defaultdict
from typing import List, Dict

from fastapi import Request, Depends, Response, APIRouter, Query

from fastapi_crud_orm_connector.api.query_parser import json_parser
from fastapi_crud_orm_connector.orm.crud import DataSort, DataSortType, MetadataTreeRequest, MetadataRequest, Crud
from fastapi_crud_orm_connector.orm.pandas_crud import PandasCrud


class GenericRouter:
    def __init__(self, crud):
        self.crud = crud

    def get_all(self, *args): pass

    def details(self, *args): pass

    def create(self, *args): pass

    def edit(self, *args): pass

    def delete(self, *args): pass

    def metadata_get_all(self, *args): pass

    def tree(self, *args): pass


class DefaultAdminRouter(GenericRouter):
    def __init__(self, crud: Crud, metadata_crud: PandasCrud = None):
        super().__init__(crud)
        self.metadata_crud = metadata_crud

    def get_all(self,
                request: Request,
                response: Response,
                data_filter=Depends(json_parser(Query('{}', alias='filter'), return_type=Dict)),
                data_range=Depends(json_parser(Query('[]', alias='range'), return_type=List, default=[0, 100])),
                data_sort=Depends(json_parser(Query('[]', alias='sort'), return_type=List)),
                data_fields=Depends(json_parser(Query('[]', alias='fields'), return_type=List)),
                convert2schema=True,
                ):
        params = dict()
        if data_sort and data_sort[0]:
            params['data_sort'] = DataSort(field=data_sort[0], type=DataSortType[data_sort[1]])
        params['limit'] = limit = data_range[1] - data_range[0] + 1
        params['offset'] = offset = data_range[0]
        get_all_response = self.crud.get_all(data_filter=data_filter, **params, data_fields=data_fields, convert2schema=convert2schema)

        # This is necessary for react-admin to work
        response.headers["Content-Range"] = f"{offset}-{offset + limit}/{get_all_response.count}"

        return get_all_response.list

    def details(self,
                request: Request,
                id: int, ):
        return self.crud.get(id)

    def create(self,
               request: Request,
               generic):
        return self.crud.create(generic)

    def edit(self,
             request: Request,
             id: int,
             generic):
        return self.crud.edit(id, generic)

    def delete(self,
               request: Request,
               id: int, ):
        self.crud.delete(id)
        return dict()

    def metadata_get_all(self,
                         request: Request,
                         response: Response,
                         metadata_request: MetadataRequest,
                         ):
        metadata = self.metadata_crud.get_all(
            data_filter=dict(id=metadata_request.map_fields) if metadata_request.map_fields else None,
            data_parse={"path": lambda v: v.str.split('>>')},
            convert2schema=False).list
        metadata['name'] = metadata['id']
        metadata = metadata.set_index('id').T.to_dict()
        return metadata

    def tree(self,
             request: Request,
             response: Response,
             metadata_request: MetadataTreeRequest, ):
        return self.metadata_crud.generate_tree(metadata_request)


def configure_crud_router(
        r: APIRouter,
        url: str,
        get_db=None,
        router: GenericRouter = None,
        metadata_crud=None,
        arg_map=None,
        include_response_model=True,
):
    if arg_map is None:
        arg_map = dict()
    _arg_map = defaultdict(dict)
    _arg_map.update(arg_map)

    crud = router.crud

    # args = router.crud.get_all.__func__.__kwdefaults__
    # n_args = router.crud.get_all.__func__.__code__.co_argcount
    @r.get(url,
           response_model=List[crud.schema.instance] if include_response_model else None,
           response_model_exclude_none=True,
           **_arg_map['get_all'])
    async def get_all(request: Request,
                      response: Response,
                      data_filter=Depends(json_parser(Query('{}', alias='filter'), return_type=Dict)),
                      data_range=Depends(json_parser(Query('[]', alias='range'), return_type=List, default=[0, 100])),
                      data_sort=Depends(json_parser(Query('[]', alias='sort'), return_type=List)),
                      data_fields=Depends(json_parser(Query('[]', alias='fields'), return_type=List)),
                      db=Depends(get_db)):
        router.crud.use_db(db)
        return router.get_all(request, response, data_filter, data_range, data_sort, data_fields)

    @r.get(url + "/{id}",
           response_model=crud.schema.instance if include_response_model else None,
           response_model_exclude_none=True,
           **_arg_map['details'])
    async def details(request: Request,
                      id: int,
                      db=Depends(get_db), ):
        router.crud.use_db(db)
        return router.details(request, id)

    @r.post(url,
            response_model=crud.schema.instance if include_response_model else None,
            response_model_exclude_none=True,
            **_arg_map['create'])
    async def create(request: Request,
                     generic: crud.schema.create,
                     db=Depends(get_db), ):
        router.crud.use_db(db)
        return router.create(request, generic)

    @r.put(url + "/{id}",
           response_model=crud.schema.instance if include_response_model else None,
           response_model_exclude_none=True,
           **_arg_map['edit'])
    async def edit(request: Request,
                   id: int,
                   generic: crud.schema.edit,
                   db=Depends(get_db)):
        router.crud.use_db(db)
        return router.edit(request, id, generic)

    @r.delete(url + "/{id}", response_model_exclude_none=True, **_arg_map['delete'])
    async def delete(request: Request,
                     id: int,
                     db=Depends(get_db)):
        router.crud.use_db(db)
        router.delete(request, id)
        return dict()

    if metadata_crud:
        @r.post(url + '/metadata',
                response_model=Dict if include_response_model else None,
                response_model_exclude_none=True,
                **_arg_map['metadata_get_all'])
        async def metadata_get_all(request: Request,
                                   response: Response,
                                   metadata_request: MetadataRequest,
                                   db=Depends(get_db)):
            router.crud.use_db(db)
            return router.metadata_get_all(request, response, metadata_request)

        @r.post(url + '/metadata/tree', **_arg_map['tree'])
        async def tree(request: Request,
                       response: Response,
                       metadata_request: MetadataTreeRequest,
                       db=Depends(get_db)):
            router.crud.use_db(db)
            return router.tree(request, response, metadata_request)

    return r
