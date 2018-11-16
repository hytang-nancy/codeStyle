DEFINE_uint32(concurrency, 4, "Ranking concurrency");
DEFINE_bool(is_req_redis, true, "request redis switch");
DEFINE_string(session_redis, "", "redis address");

static bool SortByCateWeight(const std::pair<uint32_t, float> &v1, const std::pair<uint32_t, float> &v2)
{
    return v1.second > v2.second;
}

static int GetTsFromDate(const std::string &dt, uint32_t &show_time)
{
    char fmt[] = "%Y-%m-%d %H:%M:%S";
    struct tm ts;

    if (strptime(dt.c_str(), fmt, &ts) == NULL) {
		return -1;
	}

    ts.tm_isdst = 0;
    show_time = mktime(&ts);
    return 0;
}


static void ParseUserRedis(const std::string &tline, std::vector<std::string> &vec)
{
    uint32_t n = tline.size();

    if (n > 2) {
        std::vector<std::string> vecTmp;
        strSplit(vecTmp, tline.substr(1, n - 2), ",");

        for (unsigned i = 0; i < vecTmp.size(); i++) {
            size_t tn = vecTmp[i].size();

            if (tn > 2) {
				vec.push_back(vecTmp[i].substr(1, tn - 2));
			}
        }
    }
}

static std::string set_reason(const std::string &doc_reason, float fm_score, float dnn_score, float score)
{
    std::stringstream ss;
    ss << doc_reason << "_" << fm_score << "_" << dnn_score << "_" << score;

    return ss.str();
}

static void *calculator(void *arg)
{
    ThreadParam *param = (ThreadParam *)arg;
    Ranking *ranking = param->ranking;
    google::protobuf::RepeatedPtrField<qqnews::DocInfo> &docs = *param->docs;
    std::string MultiRecall;
    NewsCtrRes ctr;

    param->result.ctrs.clear();
    param->result.ctrs.reserve(param->end - param->begin);

    int ret = 0;
    for (int i = param->begin; i < param->end; ++i) {
        MultiRecall = "";

        for (int j = 0; j < docs[i].property_ex_size(); ++j) {
            if (docs[i].property_ex(j).has_key() && docs[i].property_ex(j).key() == "reason_flag_list") {
                MultiRecall = docs[i].property_ex(j).value();
                break;
            }
        }

		ret = ranking->CtrPredict(param, docs[i].id(), docs[i].reason(), MultiRecall, &ctr);
        if (ret != 0) {
            LOG(ERROR) << "CtrPredict failed, newsid: " << docs[i].id() << ", ret:" << ret;
            continue;
        }

        ctr.idx = i;

        // set model score.
        docs[i].set_user_model_score(ctr.score);
        docs[i].set_reason(set_reason(docs[i].reason(), ctr.fm_score, ctr.dnn_score, ctr.score));

        param->result.ctrs.push_back(std::move(ctr));
    }

    return NULL;
}

static ThreadResult parallel_run(void *(*calculator)(void *), ThreadParam *params, int threads_num)
{
    ThreadResult result;

    int doc_size = params[0].docs->size();
    int seg_len = doc_size / threads_num;
    int remainder = doc_size % threads_num;
    bthread_t threads[threads_num];
    result.ctrs.reserve(doc_size);

    for (int i = 0, begin = 0; i < threads_num; ++i) {
        params[i].begin = begin;
        params[i].end = remainder-- > 0 ? (begin + seg_len + 1) : (begin + seg_len);
        begin = params[i].end;
        //TODO: need check return value.
        bthread_start_background(&threads[i], NULL, calculator, &params[i]);
    }

    for (int i = 0; i < threads_num; ++i) {
        bthread_join(threads[i], NULL);
        // merge partial result into final result.
        result.ctrs.insert(result.ctrs.end(), params[i].result.ctrs.begin(), params[i].result.ctrs.end());
    }

    return result;
}

Ranking::Ranking(): models(FLAGS_concurrency), thread_params(FLAGS_concurrency, FLAGS_ctr_threads_num)
{
    int rv = 0;

    for (uint32_t i = 0; i < FLAGS_concurrency; ++i) {
        RegressionModel *model = models.alloc();
        rv = model->init_online(*MetaFeatureDataCenter::instance(), FLAGS_reg_data_path.c_str());
        models.free(model);

        if (rv != 0) {
            LOG(ERROR) << "RegressionModel::init failed: " << rv;
            throw __LINE__;
        }

        ThreadParam *params = thread_params.alloc();
        for (uint32_t j = 0; j < FLAGS_ctr_threads_num; ++j){
            params[j].conn_id = i;
            params[j].tid = j;
        }
        
        thread_params.free(params);
    }

    rv = indexer_news.init(SHARE_MEMORY_NAME_NEWS, NEWS_ITEM_BYTE_SIZE, 0);

    if (rv != 0) {
        LOG(ERROR) << "Indexer::init failed: " << rv;
        throw __LINE__;
    }

    rv = indexer_video.init(SHARE_MEMORY_NAME_VIDEO, VIDEO_ITEM_BYTE_SIZE, 1);

    if (rv != 0) {
        LOG(ERROR) << "Indexer::init failed: " << rv;
        throw __LINE__;
    }

    rv = indexer_push.init(PUSH_MEMORY_NAME_NEWS, PUSH_ITEM_BYTE_SIZE, 2);

    if (rv != 0) {
        LOG(ERROR) << "Indexer::init failed: " << rv;
        throw __LINE__;
    }

    ctr_threads_num = FLAGS_ctr_threads_num;
    is_upload_session = FLAGS_is_upload_session;
    session_boss_id = FLAGS_session_boss_id;
    is_upload_feature_check = FLAGS_is_upload_feature_check;
    feature_check_boss_id = FLAGS_feature_check_boss_id;
    is_req_redis = FLAGS_is_req_redis;

    if (is_req_redis) {
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_REDIS;
        options.timeout_ms = 100;
        options.max_retry = 0;
        rv = session_redis_client.Init(FLAGS_session_redis.c_str(), "la", &options);

        if (rv != 0) {
            LOG(ERROR) << "Channel::Init failed: " << rv;
            throw __LINE__;
        }
    }

    is_req_user_click_redis = FLAGS_is_req_user_click_redis;

    if (is_req_user_click_redis) {
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_REDIS;
        options.timeout_ms = 100;
        options.max_retry = 0;
        rv = user_click_redis_client.Init(FLAGS_user_click_redis.c_str(), "la", &options);

        if (rv != 0) {
            LOG(ERROR) << "Channel::Init failed: " << rv;
            throw __LINE__;
        }
    }

    //LOG(NOTICE) << "Ranking start";
}

Ranking::~Ranking()
{
}
