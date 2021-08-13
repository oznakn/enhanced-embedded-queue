import DataStore from "nedb-promises";

import { Job } from "./job";
import { State } from "./state";

export interface NeDbJob {
    _id: string;
    type: string;
    priority: number;
    data?: unknown;
    createdAt: Date;
    updatedAt: Date;
    startedAt?: Date;
    completedAt?: Date;
    failedAt?: Date;
    state?: State;
    duration?: number;
    progress?: number;
    logs: string[];
}

export type DbOptions = Nedb.DataStoreOptions;

export class JobRepository {
    protected readonly db: DataStore;

    public constructor(dbOptions: DbOptions = {}) {
        this.db = DataStore.create(dbOptions);
    }

    public async init(): Promise<void> {
        await this.db.load();
    }

    public async listJobs(state?: State): Promise<NeDbJob[]> {
        const query = (state === undefined) ? {} : { state };

        return this.db.find<NeDbJob>(query)
            .sort({ createdAt: 1 });
    }

    public async findJob(id: string): Promise<NeDbJob | null> {
        return this.db.findOne<NeDbJob>({ _id: id });
    }

    public async findInactiveJobByType(type: string): Promise<NeDbJob | null> {
        const docs = await this.db.find<NeDbJob>({ type, state: State.INACTIVE })
                .sort({ priority: -1, createdAt: 1 })
                .limit(1);

        return (docs.length === 0) ? null : docs[0];
    }

    public async isExistJob(id: string): Promise<boolean> {
        return (await this.db.count({ _id: id })) === 1;
    }

    public addJob(job: Job): Promise<NeDbJob> {
        const insertDoc = {
            _id: job.id,
            type: job.type,
            priority: job.priority,
            data: job.data,
            createdAt: job.createdAt,
            updatedAt: job.updatedAt,
            state: job.state,
            logs: job.logs,
        };

        return this.db.insert(insertDoc);
    }

    public async updateJob(job: Job): Promise<void> {
        const query = {
            _id: job.id,
        };
        const updateQuery = {
            $set: {
                priority: job.priority,
                data: job.data,
                createdAt: job.createdAt,
                updatedAt: job.updatedAt,
                startedAt: job.startedAt,
                completedAt: job.completedAt,
                failedAt: job.failedAt,
                state: job.state,
                duration: job.duration,
                progress: job.progress,
                logs: job.logs,
            },
        };

        const numAffected = await this.db.update(query, updateQuery, {});

        if (numAffected !== 1) {
            throw new Error(`update unexpected number of rows. (expected: 1, actual: ${numAffected})`);
        }
    }

    public async removeJob(id: string): Promise<void> {
        await this.db.remove({ _id: id }, {});
    }
}
