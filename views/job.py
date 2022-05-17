from flask_restful import Resource
from flask import request
from utils.job_database import JobDatabaseEngine


class JobCreateView(Resource):
    """
    Job 생성 View

    (POST)  /api/jobs   Job 생성
    """

    def post(self):
        try:
            job_id = JobDatabaseEngine().save(request.get_json())
        except Exception as e:
            return {'error': str(e)}, 400
        else:
            return {'job_id': job_id}, 201


class JobView(Resource):
    """
    Job 데이터 관리 뷰

    (GET)       /api/jobs/<int:job_id>   Job 정보 검색
    (PATCH)     /api/jobs/<int:job_id>   Job 수정
    (DELETE)    /api/jobs/<int:job_id>   Job 삭제
    """

    def get(self, job_id):
        try:
            res_data = JobDatabaseEngine().get_item(job_id)
        except ValueError:
            return {'err': 'Data Not Found'}, 404
        except Exception:
            return {'err': 'Server Error'}, 500
        else:
            return res_data, 200

    def patch(self, job_id):
        try:
            success = JobDatabaseEngine().update(job_id, request.get_json())
        except ValueError:
            return {'error': 'data not found'}, 404
        except Exception:
            return {'error': 'server error'}, 500

        if not success:
            return {'error': 'Data valid failed'}, 400
        return {'error': 'success'}, 201

    def delete(self, job_id):
        try:
            success = JobDatabaseEngine().remove(job_id)
        except Exception:
            return {'error': 'server error'}, 500

        if success:
            return {'deleted': job_id}, 204
        else:
            return {'error': 'data not found'}, 404


class JobRunView(Resource):
    """
    Job 실행 뷰

    (GET)   /api/jobs/<int:job_id>/run  실행
    """
    def get(self, job_id):
        try:
            JobDatabaseEngine().run(job_id)
        except ValueError as e:
            print(e)
            return {'err': 'job not found'}, 404
        return {'status': 'ok'}, 200
