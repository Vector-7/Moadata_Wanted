from flask import Flask
from flask_restful import Api
from views.job import JobView, JobCreateView, JobRunView

from utils.job_database import JobDatabaseEngine


def generate_jobdatabase_engine():
    return JobDatabaseEngine()


def __set_app():
    app = Flask(__name__)
    api = Api(app)

    return app, api


def __set_uris(api):
    api.add_resource(JobView, '/api/job/<int:job_id>')
    api.add_resource(JobCreateView, '/api/job/create')
    api.add_resource(JobRunView, '/api/job/run/<int:job_id>')


def get_app():
    # Set app
    generate_jobdatabase_engine()
    app, api = __set_app()
    __set_uris(api)

    return app, api


def main():
    app, api = get_app()
    generate_jobdatabase_engine()
    app.run(debug=True)


if __name__ == '__main__':
    main()
