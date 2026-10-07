HW#2 Thread Pool 기반 좌석 예매 서버

1. 조원
  조: G4
  이름 / 학번 / 역할
  김재원 / 20213086 / Server·Listener, RequestQueue·Worker, 원격 배포, 영상
  윤도훈 / 20223122 / Client·ClientMain, 로그, Verify
  김지현 / 20223097 / SeatManager, Notifier


2. 배포 정보
  Server: AWS EC2 t3.micro (eu-north-1), Ubuntu 26.04 LTS, OpenJDK 21, 포트 5000, IP ***.***.***.***
  Client: 로컬 PC (Windows 11, Java 21)에서 30개 실행
  시간대: Server/Client 모두 KST. EC2는 UTC지만 Log.java에서 Asia/Seoul로 고정함
  서버가 스톡홀름이라 응답 시간에 왕복 지연(약 250~300ms)이 포함됨


3. 구성
  Server.java        서버 시작/종료 흐름, 통계, 종료 보고서
  Listener.java      접속 수락, Selector로 소켓 30개 감시, 메시지 파싱 후 큐에 넣기, POOL 로그, Deadlock 감시
  Conn.java          연결 1개 (수신 버퍼, 소켓별 송신 Lock)
  RequestQueue.java  Listener -> Worker 큐 (Lock + Condition)
  Worker.java        Worker 10개. 큐에서 꺼내서 좌석 판정 후 응답
  SeatManager.java   좌석 100개, RESERVE / RESERVE_MULTI / CANCEL 판정
  Notifier.java      Notify Queue에서 Condition으로 대기하다 깨어나 대기자에게 NOTIFY 전송
  Log.java           로그 출력
  ClientMain.java    Client 30개를 한 프로세스 안의 스레드로 실행
  Client.java        Client 1개 (송신 스레드 + 수신 스레드)
  Verify.java        실행 후 로그로 정합성 확인
  Arguments.java     공통 실행 인자 검사

  프로그램이 생성한 서버 스레드: Listener 1(main), Worker 10, Notifier 1.
  POOL 로그와 Deadlock 감시는 별도 스레드 없이 Listener의 select(500ms) 타임아웃을 이용함.

  메시지 형식 (한 줄 = 한 메시지, \n으로 구분)
    Client -> Server: HELLO <id> / RESERVE <reqId> <seat> / RESERVE_MULTI <reqId> <s1,s2,..> / CANCEL <reqId> <seat>
    Server -> Client: RESP <reqId> SUCCESS / RESP <reqId> FAIL <사유> / RESP <reqId> WAITLISTED / NOTIFY <reqId> <seat> / BYE

  reqId는 Client가 1부터 붙이고 응답과 NOTIFY에 그대로 들어감. 응답 순서가 섞여도 reqId로 맞춤.
  Client 번호는 1~30, reqId는 1~requests이며 이미 처리한 reqId의 재사용/중복 요청은 무시함.
  NOTIFY는 단일 RESERVE의 reqId와 좌석이 모두 일치할 때 한 번만 반영함.
  NOTIFY가 WAITLISTED보다 먼저 와도 좌석 배정은 즉시 반영하고, 나중의 첫 응답과 별도로 집계함.
  TCP는 메시지 경계가 없어서 서버는 받은 데이터를 버퍼에 쌓고 \n마다 한 줄씩 처리함.


4. 컴파일 및 실행 (JDK 17 이상)
  javac --release 17 -encoding UTF-8 -d out src/*.java
  java -cp out Server --host 0.0.0.0 --port 5000
  java -cp out ClientMain --host <서버 IP> --port 5000
  java -cp out Verify --log-dir logs

  Verify 실행 전 Server.txt와 Client1~30.txt를 logs 폴더에 둠.

  인자
    --host      Server/Client 필수. Server는 바인딩 IP, Client는 서버 주소
    --port      Server/Client 필수, 1~65535
    --requests  Client당 요청 수, 기본 5000
                개발 테스트에서 줄일 경우 Server, ClientMain, Verify에 같은 값을 줘야 함
                Server는 30 x requests 건에 첫 응답을 다 보내면 종료함
    --log-dir   로그 폴더, 기본 logs
    --min-interval-ms  Client 개발용 최소 전송 간격, 기본 200ms
    --max-interval-ms  Client 개발용 최대 전송 간격, 기본 1000ms

  host는 빈 값 불가, port는 1~65535, requests와 전송 간격은 양수여야 함.
  잘못된 인자는 실행 전에 거부함.
  정식 실행은 간격 인자를 생략해 0.2~1.0초를 사용함.
  나머지(Client 30개, 큐 1000, POOL 5초, Deadlock 감시 30초, 인기 좌석 1~10번 70%)는 코드에 고정.
  최종 실행은 약 51분 걸림.


5. Thread Pool
  시작할 때 Worker 10개를 만들어 끝까지 재사용함. 요청이나 연결마다 스레드를 만들지 않음.
  Request Queue는 ArrayDeque + ReentrantLock + Condition 2개(notEmpty, notFull)로 직접 구현함.
  - 큐가 비면 Worker가 notEmpty.await()로 대기, Listener가 넣을 때 signal
  - 큐 크기 1000, 꽉 차면 Listener가 notFull로 대기하며 요청은 버리지 않음
  - 꽉 찬 상태에서도 1초마다 깨어나 POOL/Deadlock 감시는 계속함

  장점: 스레드 생성 비용이 없고 부하가 늘어도 Worker 수가 고정됨. 요청도 버리지 않음.
  단점: 인기 좌석에 요청이 몰리면 Worker들이 같은 Lock을 기다리느라 다른 요청도 늦어질 수 있음.
        큐가 꽉 차 있는 동안은 Listener가 다른 소켓을 바로 읽지 못할 수 있음.
        최종 실험에서는 Queue 최대 길이가 작아서 이 상황은 발생하지 않았음.


6. 동시성 제어
  좌석마다 ReentrantLock 1개 (100개). owner와 waitlist는 그 좌석 Lock 안에서만 읽고 씀.
  Critical Section은 좌석 상태 확인 + 변경 부분임.

    RESERVE: 비었으면 배정 / 이미 내 것·대기 중이면 FAIL / 다른 Client 소유면 Waitlist 등록
    RESERVE_MULTI: 필요한 Lock을 다 잡고 전부 비었으면 전체 배정, 하나라도 아니면 전체 FAIL
    CANCEL: owner 확인 -> 해제 -> 대기자 있으면 맨 앞 Client에게 바로 넘김

  확인과 변경을 같은 Lock 안에서 해서 두 Worker가 동시에 같은 좌석을 배정하는 일을 막음.
  범위·개수 검사는 Lock 전에 하고, 응답 전송과 로그는 Lock을 푼 뒤에 함.
  Lock 해제는 finally에서 처리함.
  그 외 Lock은 Request Queue, Notify Queue, 소켓 송신, 로그 파일, Client 내부 상태에 사용함.
  좌석 Lock을 잡은 상태에서 다른 Lock을 추가로 잡지 않도록 구성함.
  Lock 경합 횟수는 tryLock()을 먼저 시도했다가 실패한 횟수로 측정함.


7. Lock Ordering (Deadlock 회피)
  RESERVE_MULTI 처리 순서
    1) 좌석 수 2~4, 범위 1~100 검사
    2) 좌석 번호 오름차순 정렬 ([5,3] -> [3,5]), 중복 검사
    3) 정렬된 순서대로 Lock 획득
    4) 전부 비었으면 전부 배정, 아니면 아무것도 안 바꾸고 FAIL
    5) finally에서 잡은 Lock 전부 해제

  모든 Worker가 작은 번호부터 Lock을 잡기 때문에 순환 대기가 생기지 않음.
  Lock을 2개 이상 잡는 곳은 RESERVE_MULTI뿐이고 나머지는 한 번에 하나만 잡음.
  Deadlock 횟수는 큐에 요청이 있는데 30초 동안 처리 완료 수가 늘지 않으면 1회로 셈.
  이 검사는 감시용이고 Deadlock을 타임아웃이나 강제 종료로 해결하지 않음.

  장점: 정렬만 하면 돼서 단순함.
  단점: 필요한 Lock을 미리 알아야 하고, 앞 좌석 Lock을 잡은 채 뒤 좌석을 기다리면
        다른 요청도 앞 좌석에서 기다릴 수 있음.


8. Waitlist / Condition Variable / Notifier
  좌석마다 ArrayDeque 기반 FIFO Waitlist를 둠.
  단일 RESERVE에서 다른 Client가 이미 좌석을 가지고 있으면 맨 뒤에 넣고 WAITLISTED를 바로 응답함.
  Worker는 좌석이 빌 때까지 기다리지 않고 다음 요청으로 넘어감.

  CANCEL 성공 시 좌석 Lock 안에서 pollFirst()로 맨 앞 대기자를 꺼내 바로 배정함.
  그래서 좌석이 잠시 EMPTY가 되는 구간이 없고 다른 Client가 중간에 끼어들 수 없음.
  pollFirst()로 꺼낸 순서대로 바로 배정하므로 Waitlist 등록 순서가 유지됨.
  NOTIFY 도착 순서가 달라도 실제 좌석 배정 순서는 바뀌지 않음.

  배정 후 Worker는 좌석 Lock을 풀고 Notify Queue에 작업을 넣고 signal함.
  Notifier는 hasWork.await()로 대기하다가 깨어나 NOTIFY를 보내고 대기시간을 기록함.
  종료 시 남은 Notify 작업을 다 처리한 뒤 끝남.
  Worker의 Request Queue 대기와 Notifier 대기 모두 Condition Variable을 사용하고 sleep polling은 하지 않음.


9. 테스트 및 정합성 확인
  테스트
  - 요청 좌석이 1~10번에 몰리게 해서 실제 약 63%가 인기 좌석에 집중되도록 함.
  - 개발 중에는 코드를 임시로 수정해 인기 좌석을 1~5번으로 제한하고,
    요청 간격을 1~20ms로 줄여 더 강한 경합 상황에서도 여러 번 테스트함.
  - 모든 테스트에서 이중예약은 0건이었음.
  - Client가 RESERVE_MULTI 좌석을 정렬하지 않고 보내고,
    Server LOCK 로그로 실제 Lock은 항상 오름차순으로 획득하는지 확인함.
  - Deadlock은 발생하지 않았음.
  - 실행 중 jstack으로 프로그램이 생성한 스레드가 Listener 1, Worker 10, Notifier 1인지 확인함.

  정합성 확인
  Verify.java로 Server.txt와 Client1~30.txt를 비교하고 결과는 logs/VerifyResult.txt에 저장함.

    1) 이중예약 0건
    2) 배정 수 - 해제 수 = 종료 시 예약 좌석 수
    3) Server 최종 좌석 = Client 30개 최종 보유 좌석
    4) WAITLISTED 수 = NOTIFY 수신 수 + 종료 시 미해결 대기 수
    5) 모든 Client가 5,000건을 보내고 첫 응답을 모두 받은 뒤 BYE를 받았는지 확인
    6) Server 처리 수와 Client 응답 집계가 일치하는지 확인
    7) 응답/통지 전송 실패와 Server 처리 예외가 0건인지 확인
    8) Deadlock 0건 및 Server 정상 종료 확인

  최종 좌석 목록이나 필수 지표가 누락된 로그는 FAIL이며 Verify 종료 코드는 1임.
  Server 로그의 자체 검사 결과는 참고용이고,
  Client 로그와 대조한 최종 PASS 여부는 Verify에서 판정함.

  집계 기준: 좌석이 Client에게 배정될 때마다 배정 +1, CANCEL 성공마다 해제 +1
            Waitlist 대기자에게 넘기는 경우는 해제 1 + 배정 1

  실측 결과 (원격 구성, 30 x 5,000건)
    처리량                   ______ req/s
    Request Queue 최대 길이   ______ 건
    이중예약                  ______ 건
    Deadlock                  ______ 건
    Waitlist 평균 대기시간    ______ sec
    Lock 경합                 ______ 건
    최종 좌석 정합성          ______

    총 요청                   150,000건
    SUCCESS                   ______ 건 (______%)
    FAIL                      ______ 건 (______%)
    WAITLISTED                ______ 건 (______%)
    NOTIFY / 미해결           ______ 건 / ______ 건
    평균 응답 시간            ______ ms

  참고: 원격 리허설 30 x 100건
    처리량 47.8 req/s, Queue 최대 12, 이중예약 0, Deadlock 0,
    Waitlist 평균 대기 13.8s, Lock 경합 0, 최종 좌석 정합성 PASS,
    SUCCESS 36.3%, FAIL 45.4%, WAITLISTED 18.2%, 평균 응답 301ms

  실제 요청 속도인 초당 약 50건에서는 Worker가 동시에 같은 좌석 Lock을 잡는 경우가 많지 않아
  Lock 경합 수가 작게 측정되었음.


10. 직접 정한 것들
  - Request Queue 크기 1000. 꽉 차면 Listener가 기다리고 요청은 버리지 않음.
    요청을 버리면 전체 150,000건의 요청/응답 수가 맞지 않을 수 있기 때문임.

  - 요청 비율: 좌석이 없을 때 RESERVE 60%, RESERVE_MULTI 40%,
    좌석을 보유하고 있을 때 RESERVE 30%, RESERVE_MULTI 20%, CANCEL 50%.
    보유 좌석이 5개 이상이면 CANCEL을 우선함.

  - 인기 좌석은 70% 확률로 1~10번을 선택함.
    60%로 했을 때 CANCEL 영향으로 전체 인기 좌석 요청 비율이 약 53%로 나와 70%로 조정함.

  - RESERVE_MULTI는 2~4개의 좌석을 중복 없이 무작위로 고르고 정렬하지 않은 순서로 보냄.
    실제 Lock Ordering은 Server에서 처리함.

  - Client는 SUCCESS 응답이나 NOTIFY를 받은 좌석만 보유 좌석으로 처리함.
    응답 대기 요청은 reqId별로 저장하고 WAITLISTED 요청과 첫 응답 전 NOTIFY를 따로 관리함.
    CANCEL이 FAIL이면 기존 보유 상태를 유지함.

  - CANCEL 응답을 기다리는 좌석에는 새로운 요청을 보내지 않음.
    개발 중 두 응답의 순서가 뒤바뀌면서 Client 보유 상태가 달라지는 문제를
    Verify에서 발견해서 이 처리를 추가함.

  - 최초 연결 시 Server가 준비되지 않은 경우 1초 간격으로 최대 30번 재시도함.
    한 번 연결된 뒤 실행 중 연결이 끊기면 재접속하지 않고 해당 실행은 무효로 보고 다시 실행함.
    모든 Client 연결이 끊기면 Server는 종료 절차를 진행함.

  - Server.txt 약 40MB, Client 로그 전체 약 26MB이며 줄이지 않고 zip으로 압축해 약 7MB로 제출함.
  - 화면에는 시작, 전체 접속 완료, 경고, 최종 결과만 출력하고 나머지는 파일에 기록함.
  - 영상은 원격 Server 실행 -> Client 30개 접속 -> 다중 예약 로그 -> 종료 후 Verify PASS 순서로 구성함.


11. 기타
  - Verify.java로 정합성을 자동 확인함.
  - 형식이 잘못된 메시지가 들어와도 Server가 종료되지 않고 FAIL로 응답함.
    이미 처리한 중복 reqId는 3장의 처리 방식대로 무시함.
  - Worker 처리 중 예외가 발생해도 가능한 경우 Client에게 FAIL 응답을 보냄.
  - Client가 응답을 읽지 않아 Server 송신이 10초 이상 진행되지 않으면 연결을 종료함.
  - 송신 재시도는 Selector의 쓰기 가능 이벤트를 사용하며 sleep polling은 하지 않음.
  - 처리량은 실제 첫 응답 전송 성공 건수만 사용함.
  - 응답 전송 실패는 response_failed로 따로 기록하고 종료 검사에서 FAIL 처리함.
  - 실행 시간 측정은 각 노드의 System.nanoTime()을 사용하고 로그 시각은 KST wall-clock을 사용함.


12. 제출물 (G4HW2.zip)
  src/
  Readme.txt
  AllDefinedLogs.txt
  logs/Server.txt
  logs/Client1.txt ~ Client30.txt
  logs/VerifyResult.txt
  download.txt