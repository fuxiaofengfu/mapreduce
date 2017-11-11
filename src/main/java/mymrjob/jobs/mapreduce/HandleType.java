package mymrjob.jobs.mapreduce;

/**
 * 处理方式枚举
 * COMBINER//combiner压缩mapper输出
 * 每种处理方式都需要进行combiner以减轻reducer数据处理量
 */
public enum HandleType {

    NULL,//无任何处理
	WORD_COUNT,//单词统计
	CONTEXT_MAX,//求全局最大
	CONTEXT_MIN,//全局最小
	CONTEXT_MAX_MIN,//同时求全局最大，最小
	GROUP_MAX,//分组最大
	GROUP_MIN,//分组最小
	GROUP_MAX_MIN,//同时求分组最大最小
	MULTIPLE_OUTPUT,//多目录输出
	MULTIPLE_INPUT,//多目录输入
	MULTIPLE_INPUT_MAPPER,//多目录多mapper处理
	SORT_ASC,//升序排序
	SORT_DESC,//降序排序
	INNER_JOIN,//多文件innerJoin
	SEMI_JOIN//缓存文件输入

}
