import Mock from 'mockjs2'

const { mock } = Mock

const get = (api, temp) => mock(api, 'get', temp)
const post = (api, temp) => mock(api, 'post', temp)

const list = [
    'myfunc',
    'abcd'
]

const createFunc = ''

const info = {
    'tenant': 'public',
    'namespace': 'default',
    'name': 'myfunc',
    'className': 'com.functionstream.ExclamationFunction',
    'inputSpecs': {
        'persistent://public/default/input-topic': {
            'schemaProperties': {},
            'consumerProperties': {},
            'regexPattern': false
        }
    },
    'output': 'persistent://public/default/output-topic',
    'producerConfig': {
        'useThreadLocalProducers': false,
        'batchBuilder': ''
    },
    'processingGuarantees': 'ATLEAST_ONCE',
    'retainOrdering': false,
    'retainKeyOrdering': false,
    'forwardSourceMessageProperty': true,
    'userConfig': {},
    'runtime': 'JAVA',
    'autoAck': true,
    'parallelism': 1,
    'resources': {
        'cpu': 1,
        'ram': 1073741824,
        'disk': 10737418240
    },
    'cleanupSubscription': true
}

const stats = {
    'receivedTotal': 0, // 收到的总数
    'processedSuccessfullyTotal': 0, // 已成功处理总计
    'systemExceptionsTotal': 0, // 系统异常总数
    'userExceptionsTotal': 0, // 用户例外总数
    'avgProcessLatency': null, // 平均进程延迟
    '1min': { // 1分钟？？
        'receivedTotal': 0,
        'processedSuccessfullyTotal': 0,
        'systemExceptionsTotal': 0,
        'userExceptionsTotal': 0,
        'avgProcessLatency': null
    },
    'lastInvocation': null, // 负载
    'instances': [ // 实例
        {
            'instanceId': 0, // 实例ID
            'metrics': { // 指标
                'receivedTotal': 0,
                'processedSuccessfullyTotal': 0,
                'systemExceptionsTotal': 0,
                'userExceptionsTotal': 0,
                'avgProcessLatency': null,
                '1min': {
                    'receivedTotal': 0,
                    'processedSuccessfullyTotal': 0,
                    'systemExceptionsTotal': 0,
                    'userExceptionsTotal': 0,
                    'avgProcessLatency': null
                },
                'lastInvocation': null,
                'userMetrics': {}
            }
        }
    ]
}

const status = {
    'numInstances': 1,
    'numRunning': 1,
    'instances': [
        {
            'instanceId': 0,
            'status': {
                'running': true,
                'error': '',
                'numRestarts': 0,
                'numReceived': 0,
                'numSuccessfullyProcessed': 0,
                'numUserExceptions': 0,
                'latestUserExceptions': [],
                'numSystemExceptions': 0,
                'latestSystemExceptions': [],
                'averageLatency': 0,
                'lastInvocationTime': 0,
                'workerId': 'c-standalone-fw-localhost-8080'
            }
        }
    ]
}

const trigger = (option) => {
    console.log(option)
    const { body } = option
    const { data: { data } } = JSON.parse(body)
    console.log(JSON.parse(body))

    return `${data}!!!`
}

const startFunc = { result: 0 }

const stopFunc = { result: 0 }

const deleteFunc = { result: 0 }

get(/\/admin\/v3\/functions\/public\/default/, list)
get(/\/admin\/v3\/functions\/public\/default\/[^/]*/, info)
get(/\/admin\/v3\/functions\/public\/default\/[^/]*\/stats/, stats)
get(/\/admin\/v3\/functions\/public\/default\/[^/]*\/status/, status)
post(/\/admin\/v3\/functions\/public\/default\/[^/]*\/trigger/, trigger)
post(/\/admin\/v3\/functions\/public\/default\/[^/]*\/delete/, deleteFunc)
post(/\/admin\/v3\/functions\/public\/default\/[^/]*/, createFunc)
post(/\/admin\/v3\/functions\/public\/default\/[^/]*\/start/, startFunc)
post(/\/admin\/v3\/functions\/public\/default\/[^/]*\/stop/, stopFunc)
