import { EventEmitter } from "events";

import { Mutex } from "enhanced-await-semaphore";
import { v4 as uuid } from "uuid";

import { Event } from "./event";
import { Job } from "./job";
import { DbOptions, JobRepository, NeDbJob } from "./jobRepository";
import { Priority } from "./priority";
import { State } from "./state";
import { Worker } from "./worker";

export interface CreateJobData {
    type: string;
    priority?: Priority;
    data?: unknown;
}

export type Processor = (job: Job) => Promise<unknown>;

interface WaitingWorkerRequest {
    resolve: (value: Job) => void;
    reject: (error: Error) => void;
    stillRequest: () => boolean;
}

export class Queue extends EventEmitter {
    public static async createQueue(dbOptions?: DbOptions): Promise<Queue> {
        const queue = new Queue(dbOptions);

        await queue.repository.init();

        await queue.cleanupAfterUnexpectedlyTermination();

        return queue;
    }

    protected static sanitizePriority(priority: number): Priority {
        switch (priority) {
            case Priority.LOW:
            case Priority.NORMAL:
            case Priority.MEDIUM:
            case Priority.HIGH:
            case Priority.CRITICAL:
                return priority;
        }

        console.warn(`Invalid Priority: ${priority}`);
        return Priority.NORMAL;
    }

    protected readonly repository: JobRepository;

    // tslint:disable:variable-name
    protected _workers: Worker[];
    // tslint:disable:variable-name

    protected waitingRequests: { [type: string]: WaitingWorkerRequest[] };

    protected requestJobForProcessingMutex: Mutex;

    public get workers(): Worker[] {
        return [...this._workers];
    }

    protected constructor(dbOptions?: DbOptions) {
        super();

        this.repository = new JobRepository(dbOptions);
        this._workers = [];
        this.waitingRequests = {};
        this.requestJobForProcessingMutex = new Mutex();
    }

    public async createJob(data: CreateJobData): Promise<Job> {
        const now = new Date();

        const job = new Job(
            Object.assign(
                {},
                data,
                {
                    queue: this,
                    id: uuid(),
                    createdAt: now,
                    updatedAt: now,
                    logs: [],
                    saved: false,
                }
            )
        );

        return await job.save();
    }

    public process(type: string, processor: Processor, concurrency: number): void {
        for (let i = 0; i < concurrency; i++) {
            const worker = new Worker({
                type,
                queue: this,
            });

            worker.start(processor);

            this._workers.push(worker);
        }
    }

    public async shutdown(timeoutMilliseconds: number, type?: string | undefined): Promise<void> {
        const shutdownWorkers: Worker[] = [];

        for (const worker of this._workers) {
            if (type !== undefined && worker.type !== type) {
                continue;
            }

            await worker.shutdown(timeoutMilliseconds);

            shutdownWorkers.push(worker);
        }

        this._workers = this._workers.filter(
            (worker) => {
                return shutdownWorkers.includes(worker) === false;
            }
        );
    }

    public async findJob(id: string): Promise<Job | null> {
        try {
            const neDbJob = await this.repository.findJob(id);

            if (neDbJob === null) {
                return null;
            }

            return this.convertNeDbJobToJob(neDbJob);
        }
        catch (error) {
            this.emit(Event.Error, error);
            throw error;
        }
    }

    public async listJobs(state?: State): Promise<Job[]> {
        try {
            return await this.repository.listJobs(state).then((docs) => {
                return docs.map((neDbJob) => this.convertNeDbJobToJob(neDbJob));
            });
        }
        catch (error) {
            this.emit(Event.Error, error);
            throw error;
        }
    }

    public async removeJobById(id: string): Promise<void> {
        let neDbJob: NeDbJob | null;
        try {
            neDbJob = await this.repository.findJob(id);
        }
        catch (error) {
            this.emit(Event.Error, error);
            throw error;
        }

        if (neDbJob === null) {
            throw new Error(`Job(id:${id}) is not found.`);
        }

        const job = this.convertNeDbJobToJob(neDbJob);

        try {
            return await job.remove();
        }
        catch (error) {
            this.emit(Event.Error, error, job);
            throw error;
        }
    }

    public async removeJobsByCallback(callback: (job: Job) => boolean): Promise<Job[]> {
        const removedJobs: Job[] = [];

        let job: Job | undefined;

        try {
            const neDbJobs = await this.repository.listJobs();

            for (const neDbJob of neDbJobs) {
                job = this.convertNeDbJobToJob(neDbJob);

                if (callback(job)) {
                    removedJobs.push(job);
                    await job.remove();
                }

                job = undefined;
            }
        }
        catch (error) {
            this.emit(Event.Error, error, job);
            throw error;
        }

        return removedJobs;
    }

    /** @package */
    public async requestJobForProcessing(type: string, stillRequest: () => boolean): Promise<Job | null> {
        // すでにジョブの作成を待っているリクエストがあれば、行列の末尾に足す
        if (this.waitingRequests[type] !== undefined && this.waitingRequests[type].length > 0) {
            return new Promise<Job>((resolve, reject) => {
                this.waitingRequests[type].push({ resolve, reject, stillRequest });
            });
        }

        // 同じジョブを多重処理しないように排他制御
        const releaseMutex = await this.requestJobForProcessingMutex.acquire();
        try {
            const neDbJob = await this.repository.findInactiveJobByType(type);

            if (neDbJob === null) {
                if (this.waitingRequests[type] === undefined) {
                    this.waitingRequests[type] = [];
                }

                return new Promise<Job>((resolve, reject) => {
                    this.waitingRequests[type].push({ resolve, reject, stillRequest });
                });
            }

            if (stillRequest()) {
                const job = this.convertNeDbJobToJob(neDbJob);

                await job.setStateToActive();

                return job;
            }
            else {
                return null;
            }
        }
        catch (error) {
            this.emit(Event.Error, error);
            throw error;
        }
        finally {
            releaseMutex();
        }
    }

    /** @package */
    public async isExistJob(job: Job): Promise<boolean> {
        return await this.repository.isExistJob(job.id);
    }

    /** @package */
    public async addJob(job: Job): Promise<void> {
        try {
            const neDbJob = await this.repository.addJob(job);

            if (this.waitingRequests[job.type] === undefined) {
                return;
            }

            let processRequest: WaitingWorkerRequest | undefined = undefined;
            while (processRequest === undefined) {
                const headRequest = this.waitingRequests[job.type].shift();

                if (headRequest === undefined) {
                    break;
                }

                if (headRequest.stillRequest()) {
                    processRequest = headRequest;
                }
            }

            if (processRequest === undefined) {
                return;
            }

            const addedJob = this.convertNeDbJobToJob(neDbJob);

            await addedJob.setStateToActive();

            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            process.nextTick(() => processRequest!.resolve(addedJob));
        }
        catch (error) {
            this.emit(Event.Error, error, job);
            throw error;
        }
    }

    /** @package */
    public async updateJob(job: Job): Promise<void> {
        try {
            return await this.repository.updateJob(job);
        }
        catch (error) {
            this.emit(Event.Error, error, job);
            throw error;
        }
    }

    /** @package */
    public async removeJob(job: Job): Promise<void> {
        try {
            return await this.repository.removeJob(job.id);
        }
        catch (error) {
            this.emit(Event.Error, error, job);
            throw error;
        }
    }

    protected async cleanupAfterUnexpectedlyTermination(): Promise<void> {
        const jobsNeedCleanup = await this.listJobs(State.ACTIVE);

        for (const job of jobsNeedCleanup) {
            await job.setStateToFailure(new Error("unexpectedly termination"));
        }
    }

    protected convertNeDbJobToJob(neDbJob: NeDbJob): Job {
        return new Job({
            queue: this,
            id: neDbJob._id,
            type: neDbJob.type,
            priority: Queue.sanitizePriority(neDbJob.priority),
            data: neDbJob.data,
            createdAt: neDbJob.createdAt,
            updatedAt: neDbJob.updatedAt,
            startedAt: neDbJob.startedAt,
            completedAt: neDbJob.completedAt,
            failedAt: neDbJob.failedAt,
            state: neDbJob.state,
            duration: neDbJob.duration,
            progress: neDbJob.progress,
            logs: neDbJob.logs,
            saved: true,
        })
    }
}
