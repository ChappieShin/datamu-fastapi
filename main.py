from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any
from rapidfuzz import process, fuzz, utils
import pandas as pd
import numpy as np

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

@app.post('/api/append-table')
async def append_table(data: Dict[str, Any]):
    try:
        table_1 = pd.DataFrame(data['table_1'])
        table_2 = pd.DataFrame(data['table_2'])
        append_type = data['append_type']

        if append_type == 'vertical':
            if not table_1.columns.equals(table_2.columns):
                raise ValueError('Unable to append (Columns do not match)')
            result = pd.concat([table_1, table_2], ignore_index=True)
        elif append_type == 'horizontal':
            if any(col in table_1.columns for col in table_2.columns):
                raise ValueError('Unable to append (Columns contain duplicates)')
            result = pd.concat([table_1, table_2], axis=1)
        else:
            raise ValueError('Invalid append_type parameter')
        
        result.replace({np.nan: None}, inplace=True)

        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post('/api/join-table')
async def join_table(data: Dict[str, Any]):
    try:
        table_1 = pd.DataFrame(data['table_1'])
        table_2 = pd.DataFrame(data['table_2'])
        join_col_1 = data['join_col_1']
        join_col_2 = data['join_col_2']
        join_col_name = data.get('join_col_name')
        join_type = data['join_type']

        result = pd.merge(table_1, table_2, left_on=join_col_1, right_on=join_col_2, how=join_type)

        if not join_col_1 == join_col_2 and len(result) > 0:
            if join_type == 'right':
                result.drop(join_col_1, axis=1, inplace=True)
            else:
                result.drop(join_col_2, axis=1, inplace=True)

        if join_col_name and len(result) > 0:
            if join_type == 'right':
                result.rename(columns={join_col_2: join_col_name}, inplace=True)
            else:
                result.rename(columns={join_col_1: join_col_name}, inplace=True)

        result.replace({np.nan: None}, inplace=True)

        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post('/api/cluster-matching')
async def cluster_matching(data: Dict[str, Any]):
    try:
        table = pd.DataFrame(data['table'])
        column = data['col']
        cluster_col_name = data.get('cluster_col_name')
        replace_col = data['replace_col']
        print(replace_col)

        def find_best_match(value, choices):
            return process.extractOne(value, choices, scorer=fuzz.WRatio, processor=utils.default_process)[0]
        
        clustered_column = table[column].apply(lambda x: find_best_match(x, table[column].tolist()))
        table.insert(table.columns.get_loc(column) + 1, column + ' (clustered)', clustered_column)

        if (replace_col):
            table.drop([column], axis=1, inplace=True)

        if (cluster_col_name):
            table.rename(columns={column + ' (clustered)': cluster_col_name}, inplace=True)


        return table.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app)