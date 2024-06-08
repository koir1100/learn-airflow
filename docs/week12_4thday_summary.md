## Airflow 고급 기능  
  
### 개념 요약  

1. Dag 란?  
  - Directed Acyclic Graph의 줄임말로, Airflow에서 데이터 파이프라인 구축을 통칭하는 ETL을 의미  
  - 하나의 Dag는 한 개 이상의 Task로 구성되며, 세 개의 태스크로 구성할 때, Extract, Transform, Load로 구성 가능  
    - Task는 오퍼레이터로 만들어지며, 기 구현된 오퍼레이터 사용 또는 직접 구현 가능  
2. Dag 실행 방법  
  - 주기적으로 실행하는 방법으로 schedule을 지정하는 방식  
  - 다른 Dag에 의해 Trigger되어 실행되는 방식  
    - 명시적 트리거: 어떤 Dag가 분명하게 다른 Dag를 트리거 (TriggerDagRunOperator - 맥스 님 PICK!)  
    - Reactive 트리거: 다른 Dag의 특정 작업이 완료될 때까지 대기하고, 완료됨을 확인한 경우 실행 (ExternalTaskSensor - 비추천)  
      - 이는 어떤 Dag가 다른 Dag의 특정 작업의 완료를 확인하는 것을 정작 다른 Dag 본인은 모름  
  - 상황에 따른 태스크 실행 방식  
    - BranchPythonOperator: 조건에 따라 다른 태스크로 분기  
    - LatestOnlyOperator: 과거 데이터에 대한 Backfill 수행 시 되풀이하면 안 되는 (불필요한) 태스크 처리  
    - Trigger Rule 기반 실행: 앞단 Task가 실패하더라도 뒷단 Task의 실행이 필요한 경우 trigger_rule 파라미터 기반 결정 가능  
3. Sensor  
  - Reactive한 트리거로, 특정 조건을 만족할 때까지 대기하는 Operator임  
  - FileSensor, TimeSensor, ExternalTaskSensor
    - [airflow.sensors.filesystem](https://airflow.apache.org/docs/apache-airflow/2.5.1/_api/airflow/sensors/filesystem/index.html#airflow.sensors.filesystem.FileSensor), [airflow.sensors.time_sensor](https://airflow.apache.org/docs/apache-airflow/2.5.1/_api/airflow/sensors/time_sensor/index.html#airflow.sensors.time_sensor.TimeSensor), [airflow.sensors.external_task](https://airflow.apache.org/docs/apache-airflow/2.5.1/_api/airflow/sensors/external_task/index.html#airflow.sensors.external_task.ExternalTaskSensor) 모듈에 정의  
  - HttpSensor: [airflow.providers.http.sensors.http](https://airflow.apache.org/docs/apache-airflow-providers-http/4.1.1/_api/airflow/providers/http/sensors/http/index.html#airflow.providers.http.sensors.http.HttpSensor) 모듈에 정의  
  - SqlSensor: [airflow.providers.common.sql.sensors.sql](https://airflow.apache.org/docs/apache-airflow-providers-common-sql/1.3.3/_api/airflow/providers/common/sql/sensors/sql/index.html#airflow.providers.common.sql.sensors.sql.SqlSensor) 모듈에 정의  
  - 두 가지 방식으로 대기하는데, 기본으로는 poke mode이며, reschedule mode도 있음.  
    - poke는 주기적으로 만족 여부를 확인하고자 worker 자원을 계속 가지며, worker의 자원 낭비 가능성이 있음  
    - reschedule은 만족하지 않는 경우 확인 주기가 올 때까지 worker를 놓아주고, 주기가 왔을 때 worker 자원을 다시 잡아서 만족 여부를 확인하는 방법임.  
      다만, 주기가 오더라도 확인을 위한 worker 자원이 부족한 경우 잡지 못하는 현상이 발생할 수 있음(utilization하나, 보장하지 못할 경우도 있을 수 있음)  
4. [Trigger Rules](https://airflow.apache.org/docs/apache-airflow/2.5.1/core-concepts/dags.html#trigger-rules)  
  - Dag의 이전 Task에 따라 다음 Task 실행을 결정하기 위한 조건  
    - 크게 all_success(기본값), all_failed, all_done, one_failed, one_success, none_failed, none_skipped, none_failed_min_one_success, always(dummy) 등이 있음.  
  - 참고로, Task의 수행 결과는 success, failed, skipped 로 구분됨.  
5. Task Grouping  
  - Task 수가 많은 Dag라면 여러 Task를 성격에 따라 묶어서 관리하고자 하는 욕구가 생기게 되는데, 이를 위해 Task Grouping이 등장하였음  
  - 또한, Task Group에 대한 중첩 및 실행 순서 정의 가능 → 이를 통한 시각적으로 복잡한 Task 구조 간소화  
6. Dynamic Dag  
  - Jinja 기반 Template 파일과 Template 파일에 전달할 매개변수를 yaml 파일에 정의 → Dag 파일을 동적으로 생성  
    - 다만, 동적으로 여러 Dag를 생성하는 것과 하나의 Dag 내 Task를 늘리는 것에 대한 고려 필요  
    - 이는, (Dag의) 오너가 다르거나 Task 수가 너무 커지는 경우라면 Dag 복제가 바람직함  
