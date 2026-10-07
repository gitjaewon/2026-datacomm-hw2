1. 조원
  조: G4
  이름 / 학번 / 역할
  김재원 / 20213086 / Server·Listener, RequestQueue·Worker, 원격 배포, 영상
  윤도훈 / 20223122 / Client·ClientMain, 로그, Verify
  김치헌 / 20223097 / SeatManager, Notifier


2. 배포 정보
  Server: AWS EC2 t3.micro (eu-north-1), Ubuntu 26.04 LTS, OpenJDK 21
  Client: 로컬 PC (Windows 11, Java 21)에서 30개 실행
  시간대: KST


3. 구성
  Server: Server(시작/종료·통계), Listener(Selector로 30개 소켓 감시·파싱·POOL/Deadlock 감시),
          RequestQueue, Worker(10개), SeatManager(좌석 100개 판정), Notifier(NOTIFY 전송), Conn, Log
  Client: ClientMain(Client 30개를 스레드로 실행), Client(송신·수신 스레드)
  기타:   Verify(로그 정합성 확인), Arguments(실행 인자 검사)
  서버 스레드: Listener 1(main), Worker 10, Notifier 1. POOL/Deadlock 감시는 select(500ms) 타임아웃 이용.

  메시지 형식 (한 줄 = 한 메시지, reqId로 요청·응답 매칭)
    Client -> Server: HELLO <id> / RESERVE <reqId> <seat> / RESERVE_MULTI <reqId> <s1,s2,..> / CANCEL <reqId> <seat>
    Server -> Client: RESP <reqId> SUCCESS|FAIL <사유>|WAITLISTED / NOTIFY <reqId> <seat> / BYE
  로그 형식과 FAIL 사유는 AllDefinedLogs.txt 참고.


4. 컴파일 및 실행 (JDK 17 이상)
  Linux (Server, EC2)
    javac --release 17 -encoding UTF-8 -d out src/*.java
    java -cp out Server --host 0.0.0.0 --port 5000

  Windows (Client, 로컬 PC)
    javac --release 17 -encoding UTF-8 -d out src\*.java
    java -cp out ClientMain --host <서버 IP> --port 5000

  정합성 확인 (실행 종료 후, Windows)
    scp -i <키.pem> ubuntu@<서버 IP>:~/hw2/logs/Server.txt logs\
    java -cp out Verify --log-dir logs
  Verify는 Server.txt와 Client1~30.txt가 logs 폴더에 모두 있어야 함.

  인자
    --host      필수. Server는 바인딩 IP, Client는 서버 주소
    --port      필수, 1~65535
    --requests  Client당 요청 수, 기본 5000 (줄이면 Server, ClientMain, Verify에 같은 값)
    --log-dir   로그 폴더, 기본 logs
  나머지(Client 30개, 큐 1000, POOL 5초, Deadlock 감시 30초)는 코드에 고정. 최종 실행 약 51분.


5. Thread Pool
  Worker 10개를 시작 시 만들어 끝까지 재사용 (요청·연결마다 스레드 생성 안 함).
  Request Queue: ArrayDeque + ReentrantLock + Condition(notEmpty, notFull) 직접 구현, 크기 1000.
    비면 Worker가 notEmpty로 대기, 꽉 차면 Listener가 notFull로 대기 (요청 안 버림, 1초마다 깨어 감시 유지).

  장점: 스레드 생성 비용 없음, 부하가 늘어도 스레드 수 고정, 요청 유실 없음.
  단점: Worker 수가 고정이라 처리량에 상한이 있고, 인기 좌석 Lock에서 Worker가 막히면 다른 요청도 늦어짐.
        큐가 꽉 차면 Listener가 대기하는 동안 다른 소켓을 읽지 못함.
        (최종 실행은 큐 최대 21, Lock 경합 8건이라 실제 영향은 거의 없음)


6. 동시성 제어
  좌석마다 ReentrantLock 1개 (100개). owner와 waitlist는 그 좌석 Lock 안에서만 접근.
  Critical Section = 좌석 상태 확인 + 변경 (RESERVE 배정/대기 등록, MULTI 전체 배정, CANCEL 해제/넘김).
  근거: 확인과 변경 사이에 다른 Worker가 끼면 둘 다 "비었다"고 보고 이중예약이 생김.
  범위 검사는 Lock 전, 응답 전송·로그는 Lock 해제 후 (Lock 시간 최소화). 해제는 finally.
  좌석 Lock을 쥔 채 다른 Lock(큐, 송신, 로그)은 잡지 않음.
  Lock 경합 횟수: tryLock() 실패 횟수.


7. Lock Ordering (Deadlock 회피)
  RESERVE_MULTI: 개수·범위·중복 검사 -> 오름차순 정렬([5,3] -> [3,5]) -> 순서대로 Lock
    -> 전부 비었으면 전부 배정, 아니면 FAIL -> finally에서 전부 해제.
  모두 작은 번호부터 잡으므로 순환 대기가 없음. Lock을 2개 이상 잡는 곳은 여기뿐.
  Deadlock 횟수: 큐에 요청이 있는데 30초간 처리가 없으면 1회 (감시만 함).

  장점: 정렬만 하면 돼서 단순함.
  단점: 필요한 Lock을 미리 알아야 하고, 앞 좌석 Lock을 쥔 채 기다리는 동안 그 좌석 요청도 막힘.


8. Waitlist / Condition Variable / Notifier
  Waitlist: 좌석마다 ArrayDeque (FIFO). 남의 좌석에 RESERVE하면 addLast로 등록하고 바로 WAITLISTED 응답.
            Worker는 기다리지 않고 다음 요청 처리.
  FIFO 보장: CANCEL 시 좌석 Lock 안에서 pollFirst로 맨 앞 대기자에게 바로 배정.
            좌석이 비는 순간이 없어 끼어들기 불가, 등록 순서 = 배정 순서.
  Condition Variable: Notifier는 hasWork.await()로 대기, Worker가 작업 넣고 signal.
            Request Queue도 notEmpty/notFull로 대기. sleep 반복 확인 없음.


9. 테스트 및 정합성 확인
  테스트
  - 요청 좌석의 약 64%가 인기 좌석 1~10번에 몰리게 함.
  - 개발 중에는 인기 좌석 1~5번, 요청 간격 1~20ms로 경합을 키워 여러 번 테스트함. 이중예약 0건.
  - RESERVE_MULTI 좌석을 정렬 안 하고 보내고, LOCK 로그로 항상 오름차순 획득 확인. Deadlock 0건.
  - jstack으로 서버 스레드가 Listener 1, Worker 10, Notifier 1뿐인지 확인.

  정합성 확인 (Verify.java, Server.txt와 Client1~30.txt 대조, 결과는 logs/VerifyResult.txt)
    1) 이중예약 0건
    2) 배정 수 - 해제 수 = 종료 시 예약 좌석 수
    3) Server 최종 좌석 = Client 30개 최종 보유 좌석
    4) WAITLISTED 수 = NOTIFY 수신 수 + 종료 시 미해결 대기 수 (통지 실패 0건 포함)
    5) 모든 Client가 5,000건을 보내고 첫 응답을 모두 받은 뒤 BYE를 받음
    6) Server 처리 수 = Client 응답 집계, 응답 전송 실패·처리 예외 0건, Server 정상 종료(server_checks=PASS)
    7) Deadlock 0건
  하나라도 실패하거나 필수 지표가 누락되면 FAIL, 종료 코드 1.

  집계 기준: 배정될 때마다 배정 +1, CANCEL 성공마다 해제 +1 (대기자에게 넘기면 해제 1 + 배정 1)

  실측 결과 (원격 구성, 30 x 5,000건)
    처리량                    49.1 req/s
    Request Queue 최대 길이   21 건
    이중예약                  0 건
    Deadlock                  0 건
    Waitlist 평균 대기시간    32.030 sec
    Lock 경합                 8 건
    최종 좌석 정합성          PASS

    총 요청                   150,000건
    SUCCESS                   55,734 건 (37.2%)
    FAIL                      75,617 건 (50.4%)
    WAITLISTED                18,649 건 (12.4%)
    NOTIFY / 미해결           18,408 건 / 241 건
    평균 응답 시간            283.9 ms



10. 직접 정한 것들
  - Request Queue 크기 1000. 꽉 차면 Listener가 대기하고 요청은 버리지 않음.
  - 요청 비율 (%)
      보유 좌석 0개:    RESERVE 60, RESERVE_MULTI 40 (취소할 좌석이 없으므로)
      보유 좌석 1~4개:  RESERVE 30, RESERVE_MULTI 20, CANCEL 50
      보유 좌석 5개 이상: CANCEL 100 (한 Client에 좌석이 쌓이는 것 방지)
  - 인기 좌석: 70% 확률로 1~10번. 60%일 때는 CANCEL 영향으로 실제 비율이 약 53%라 올림.
  - RESERVE_MULTI: 2~4석을 중복 없이 고르고 정렬 안 한 순서로 보냄 (정렬은 Server가 함).
  - Client 스레드: 송신·수신 스레드를 분리. 응답을 기다리지 않고 계속 보내고, NOTIFY도 따로 받기 위해.
  - 메시지 구분: TCP는 메시지 경계가 없어서, Server는 받은 데이터를 버퍼에 쌓고 \n 단위로 잘라 한 줄씩 처리.
  - Client 보유 좌석은 SUCCESS나 NOTIFY를 받은 좌석만. CANCEL이 FAIL이면 보유 유지.
    CANCEL 응답을 기다리는 좌석에는 새 요청을 안 보냄 (응답 순서가 뒤바뀌는 문제를 Verify로 발견해 추가).
  - 연결: 처음 접속 시 1초 간격 최대 30번 재시도. 실행 중 끊기면 재접속 안 하고 그 실행은 무효.
    전원 끊기면 Server 종료.
  

11. 기타
  - 잘못된 메시지나 Worker 예외가 있어도 Server는 죽지 않고 FAIL 응답.
  - 송신이 10초 넘게 막히면 연결 종료. 재시도는 Selector 쓰기 이벤트 사용 (sleep 없음).
  - 처리량은 첫 응답 전송 성공 건수 기준. 시간 측정은 nanoTime, 로그 시각은 KST.
