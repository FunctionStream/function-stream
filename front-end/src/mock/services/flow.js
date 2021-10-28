const info1 = {
  image: 'streamnative/pulsar-functions-go-sample:2.8.1',
  name: 'test-ex',
  autoAck: true,
  className: 'exclamation_function.ExclamationFunction',
  forwardSourceMessageProperty: true,
  MaxPendingAsyncRequests: 1000,
  replicas: 1,
  maxReplicas: 5,
  logTopic: 'persistent://public/default/logging-function-logs',
  input: {
    topics: ['persistent://public/default/a1'],
    typeClassName: 'java.lang.String'
  },
  output: {
    topic: 'persistent://public/default/a2',
    typeClassName: 'java.lang.String'
  },
  pulsar: {
    pulsarConfig: 'mesh-test-pulsar'
  },
  resources: {
    requests: {
      cpu: '0.1',
      memory: '10M'
    },
    limits: {
      cpu: '0.2',
      memory: '200M'
    }
  },
  golang: {
    go: '/pulsar/examples/go-exclamation-func'
  }
}

const info2 = {
  image: 'streamnative/pulsar-functions-go-sample:2.8.1',
  name: 'test-ex2',
  autoAck: true,
  className: 'exclamation_function.ExclamationFunction',
  forwardSourceMessageProperty: true,
  MaxPendingAsyncRequests: 1000,
  replicas: 1,
  maxReplicas: 5,
  logTopic: 'persistent://public/default/logging-function-logs',
  input: {
    topics: ['persistent://public/default/a2'],
    typeClassName: 'java.lang.String'
  },
  output: {
    topic: 'persistent://public/default/a3',
    typeClassName: 'java.lang.String'
  },
  pulsar: {
    pulsarConfig: 'mesh-test-pulsar'
  },
  resources: {
    requests: {
      cpu: '0.1',
      memory: '10M'
    },
    limits: {
      cpu: '0.2',
      memory: '200M'
    }
  },
  golang: {
    go: '/pulsar/examples/go-exclamation-func'
  }
}

export { info1, info2 }
