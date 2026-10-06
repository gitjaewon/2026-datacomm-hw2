# HW#2 데이터통신 프로그래밍 과제 정리

> **Thread Pool 기반 동시성 제어 서버 — 다중 클라이언트 실시간 좌석 예매 시스템**
>
> 출처: `HW2description (1).pdf`(공식 명세, 18p) + `HW2explanation (1).pdf`(학생용 설명 슬라이드, 31p) 교차 검증
> 두 문서가 다르면 **명세(description)를 기준**으로 정리했고, 차이점은 [맨 아래 §11](#11-두-문서-간-차이점--불일치-체크)에 모아 두었다.

| 항목 | 내용 |
|---|---|
| 배정일 | 2026.09.29(화) 10:30 |
| **마감** | **2026.10.12(월) 23:59** |
| 제출 | `G조이름HW2.zip` 1개 (예: `G2HW2.zip`), **조별 1명만** 제출 — 압축 오류 시 0점 |

---

## 0. 한 줄 요약

**원격 호스트(AWS/GCP 등)** 에서 서버 1대를 돌리고, **로컬 PC** 에서 Client 30개가 각각 5,000건(총 **150,000건**)의 예약/다중예약/취소 요청을 보낸다.
서버는 **Worker 10개 고정 Thread Pool** 로 처리하면서 **이중예약 0건**, **Deadlock 0건** 을 보장해야 한다.

| 숫자 | 의미 |
|---|---|
| 100 | 좌석 수 (1~100번) |
| 30 | 동시 접속 Client 수 |
| 5,000 | Client당 요청 수 (총 150,000) |
| 10 | Worker Thread 수 (고정) |
| 0.2~1.0초 | Client 요청 전송 간격 (평균 0.6초 → 전체 실행 약 **50분**) |

HW#1(분산, Master/Worker, 장애 내성)과 달리 HW#2는 **단일 서버 내 스레드 간 공유 자원 경합(동시성)** 문제다.

---

## 1. 핵심 개념

| 개념 | 설명 | 비유(설명 슬라이드) |
|---|---|---|
| **Thread Pool** | 서버 시작 시 Worker 10개를 미리 만들고 Request Queue에서 요청을 꺼내 처리. **10개 고정**. 요청/Client 수에 따라 생성하는 방식 불인정 | 창구 직원 10명 |
| **Request Queue** | Listener가 받은 요청을 담는 Thread-safe Queue. 크기·가득 찼을 때 동작은 조가 결정 | 창구 앞 대기줄 |
| **Listener Thread** | 연결 수락 + 다중 소켓 감시 + 파싱 + 큐 적재. 좌석은 직접 처리 안 함 | 입구 안내 직원 |
| **Critical Section / Mutex** | 좌석 정보 읽기/쓰기 구간. **좌석별 Mutex** 로 보호 | 좌석마다 달린 자물쇠 |
| **Race Condition** | 동기화 없이 같은 좌석을 동시에 읽고 쓰면 발생 → 이중예약 | |
| **원자적 다중 좌석 예약** | 2~4석 예약은 **All-or-Nothing** | |
| **Lock Ordering** | 여러 Lock은 항상 **좌석 번호 오름차순** 으로 획득 → Deadlock 방지 | "작은 번호부터" 규칙 |
| **Condition Variable / Waitlist** | 이미 예약된 좌석의 단일 RESERVE → 좌석별 Waitlist(FIFO) 등록, 취소 시 대기자에게 배정 후 NOTIFY. Worker는 Waitlist 때문에 블로킹되지 않음 | 대기번호표 + 호출벨 |

---

## 2. 시스템 구조 & 배포

```
[ Local PC ]                    [ Physical remote host (AWS/GCP etc.) ]
Client1 ... Client30 --TCP (30 conn)--> Listener Thread (1)
                                          accept + multi-socket watch + parse
                                                    |
                                                    v
                                     Request Queue (thread-safe)
                                                    |
                  +---------------------------------+---------------------------+
                  v                                 v                           v
             Worker #1                         Worker #2       ...         Worker #10
                  +---------------------------------+---------------------------+
                                                    v
                          Seat[1..100] = { owner, waitlist(FIFO) }  (per-seat Mutex)
                                                    |  on CANCEL: hand seat to waitlist head
                                                    v
            Notify Queue (Mutex + CondVar) --> Notifier Thread (1, recommended) --> Client
```

- 일반 응답(SUCCESS/FAIL/WAITLISTED)은 **Worker가 직접 소켓으로 전송**, Waitlist 통지(NOTIFY)만 Notifier가 담당.

### 2-1. 스레드 구성 규칙 (서버)
- **Worker 정확히 10개**
- 그 외 허용: **Listener 1개**, **Notifier 1개(권장)**, **Monitor 1개(선택, 상태 출력용)**
- 서버 실행 중 이 외의 스레드 추가 생성 금지 (연결마다/요청마다 스레드 생성 = **0점**)
- Client 쪽 스레드 사용은 제한 없음

### 2-2. 배포 (반드시 준수)
- Server는 **별도의 물리적 원격 호스트** (AWS, GCP 등, 무료/저가 Linux 인스턴스면 충분)
- Client 30개는 **로컬 PC** 에서 실행해 인터넷(TCP)으로 접속
- 개발·디버깅은 로컬 한 대에서 해도 됨. 단 **제출 최종 로그는 원격 구성에서 얻은 것** 이어야 함
- Server 바인딩 IP/포트, Client 접속 주소는 **하드코딩 금지** → 실행 인자 또는 설정 파일, 방법을 Readme에 명시
- 방화벽(보안 그룹)에서 포트 허용. 실험 끝나면 인스턴스 중지/삭제(비용은 조 책임)

### 2-3. 시각/시간대
- 로그 시각은 각 노드의 **wall-clock, `HH:MM:SS.mmm`**
- 지표는 **한 노드 안에서 잰 값만** 사용: 응답시간 = **Client 시계**, Waitlist 대기시간 = **Server 시계**
- 원격 호스트는 보통 UTC → 서버/Client 시간대 통일(UTC 또는 KST) **권장**, 사용한 시간대를 Readme에 기재
- (설명 슬라이드 추가) NTP 시간 동기화 권장(필수 아님), 응답시간에 네트워크 지연(예: 60ms대) 포함되는 건 정상

---

## 3. 요청 처리 규칙 (모든 조 공통)

- 좌석 번호 범위 밖(1~100), RESERVE_MULTI의 좌석 수·중복·범위 오류는 **Lock을 잡기 전에** 바로 FAIL
- 그 외 판정은 **해당 좌석의 Lock을 잡은 상태에서** 위에서부터 순서대로
- FAIL 사유 표기 방식은 자유

| 요청 | 조건 (위에서부터 판정) | 응답 | 처리 |
|---|---|---|---|
| **RESERVE** (1석) | 좌석 번호 범위 밖 | FAIL | (Lock 전) 변경 없음 |
| | 좌석이 EMPTY | SUCCESS | owner = 요청자 |
| | 요청자가 이미 owner이거나 이미 그 좌석 Waitlist에 있음 | FAIL | 변경 없음 |
| | 그 외 (다른 Client 보유 중) | **WAITLISTED** | 좌석별 Waitlist(FIFO) 맨 뒤 등록, Worker는 블로킹 없이 다음 요청 |
| **RESERVE_MULTI** (2~4석) | 좌석 수가 2~4 아님 / 중복 / 범위 밖 | FAIL | (Lock 전) 변경 없음 |
| | 요청 좌석 중 하나라도 EMPTY 아님 | FAIL | **어떤 좌석도 변경 안 함, Waitlist 등록 안 함** |
| | 요청 좌석 전부 EMPTY | SUCCESS | 전부 배정 |
| **CANCEL** (1석) | 좌석 번호 범위 밖 | FAIL | (Lock 전) 변경 없음 |
| | 요청자가 owner | SUCCESS | 대기자 없으면 EMPTY / 있으면 **Waitlist 맨 앞에게 즉시 배정 + NOTIFY** |
| | owner가 아님 | FAIL | 변경 없음 |

- **WAITLISTED도 첫 응답 → "처리 완료"로 계산.** 이후 좌석이 배정되면 **같은 요청 번호로 NOTIFY** 가 별도 이벤트로 도착
- NOTIFY가 WAITLISTED보다 먼저 도착할 수 있음 → Client는 **요청 번호로 매칭**

### 3-1. Lock 획득 ≠ 예약
Lock은 "지금 이 좌석 상태를 나만 읽고 쓸 수 있다"는 권한일 뿐. 예약은 Lock을 잡은 뒤 상태를 확인하고 owner를 바꾸는 코드다.

1. `Lock(1)` 획득 (다른 Worker가 쥐고 있으면 대기) — 아직 아무 변화 없음
2. owner, Waitlist 읽기
3. (a) EMPTY → owner=요청자 → SUCCESS / (b) 이미 owner·대기 중 → FAIL / (c) 타인 보유 → Waitlist 맨 뒤 → WAITLISTED
4. `Lock(1)` 해제 → 응답

> **확인(②)과 변경(③)은 반드시 같은 Lock 안에서.** Lock 밖에서 EMPTY를 보고 나서 Lock을 잡아 배정하면 그 사이에 뺏겨 이중예약 발생.

---

## 4. 동작 예시

### 4-1. Race Condition (좌석 #42, Client5 vs Client12, 동시 요청)

| 시각 | Mutex 미사용 (잘못된 예) | Mutex 사용 (올바른 예) |
|---|---|---|
| 01.000 | 두 Worker가 동시에 `seat[42]==EMPTY` 확인 | Worker#3, #7이 요청을 꺼냄 |
| 01.001 | 둘 다 "예약 가능"으로 판단하고 각자 배정 | Worker#3 Lock(42) → EMPTY → Client5 배정. Worker#7은 Lock 대기 |
| 01.002 | 둘 다 SUCCESS → **이중예약** (마지막 값으로 덮어씀) | Worker#3 해제 → SUCCESS. Worker#7 Lock 획득 → Client5 보유 → Client12 Waitlist 등록 |
| 01.003 | - | Worker#7 해제 → WAITLISTED |

(같은 시점에 Worker#1이 Client8의 좌석 #9를 처리하는 것은 **다른 좌석이므로 동시에 진행**)

### 4-2. 다중 좌석 & Deadlock 회피 (A: [5,3], B: [3,5])

| | Lock Ordering 미적용 | Lock Ordering 적용 (오름차순) |
|---|---|---|
| Worker A | #5 획득 → #3 대기 | 정렬 [3,5] → #3부터 시도 |
| Worker B | #3 획득 → #5 대기 | 정렬 [3,5] → #3부터 시도 |
| 결과 | 순환 대기 → **Deadlock** | 한 쪽만 #3 획득, 나머지는 대기 후 순차 처리. 먼저 쪽 SUCCESS, 나중 쪽 FAIL(이미 배정됨, Waitlist 등록 안 함) |

RESERVE_MULTI 처리 순서:
1. 요청 검사(2~4석, 중복 없음, 1~100) — 아니면 **Lock 전에** FAIL
2. 오름차순 정렬
3. 정렬 순서대로 하나씩 Lock 획득 (**어떤 경우에도 오름차순에서 벗어나지 않음**)
4. 전부 EMPTY면 일괄 배정, 하나라도 아니면 아무것도 바꾸지 않고 FAIL
5. **잡은 Lock 모두 해제** (중간 실패 시에도 반드시) → 응답·로그

### 4-3. 취소 & Waitlist 통지 (좌석 #1: owner Client2, 대기 Client9)

1. Client9가 좌석#1 RESERVE → 즉시 WAITLISTED, Waitlist 등록 (Worker는 블로킹 없이 계속)
2. Client2가 CANCEL → Worker#4가 Lock(1) → owner 확인 → 대기 1번 Client9 확인 → **EMPTY로 두지 않고 곧바로 Client9에게 배정**(끼어들기 불가)
3. Lock(1) 해제 → 통지 작업을 **Notify Queue** 에 넣고 **CV로 Notifier 깨움** → Client2에게 SUCCESS
4. Notifier가 깨어나 Client9에게 `NOTIFY(SUCCESS, 좌석#1)` 전송, 대기시간 기록. Worker#4는 이미 다음 요청 처리 중

- **FIFO 필수**: 먼저 등록한 Client부터 배정. 기준은 **NOTIFY 도착 순서가 아니라 좌석 배정 순서**. 어기면 감점
- **폴링 금지**: `while(...) sleep(0.01)` 같은 busy-waiting 금지

### 4-4. Worker 루프

| 단계 | 하는 일 | 유의점 |
|---|---|---|
| ① 꺼내기 | Request Queue에서 1건 | 비어 있으면 **CV로 잠듦** (sleep 반복 확인은 감점). 먼저 꺼낸 Worker 한 명만 처리 |
| ② Lock·판정 | 범위 확인(Lock 불필요) → 좌석 Lock → §3 규칙 판정. 다중이면 오름차순 | 같은 좌석 Lock만 서로 기다림 |
| ③ 변경·해제 | 배정/해제/Waitlist 등록 후 Lock 모두 해제 | Lock은 상태 바꾸는 순간에만(권장). 실패해도 모두 해제 |
| ④ 응답·로그 | Client에 응답, Server.txt 기록 | Lock 푼 뒤 I/O(권장). **소켓별 송신 Lock** 으로 동시 write 방지 |

참고: 초당 약 50건 도착, 1건당 수 ms → 동시에 일하는 Worker는 평균 1~2명. 핵심은 같은 좌석/겹치는 다중 좌석 요청이 동시에 올 때의 정확성.

---

## 5. 상세 요구사항

### 5-1. Server

**필수**
- 좌석 1~100 초기화, 모두 EMPTY. 좌석마다 `owner`, `waitlist(FIFO)`
- **좌석별 Mutex(좌석 1개당 Lock 1개)**. owner/waitlist는 해당 좌석 Lock 안에서만 읽고 씀. **전체 좌석 맵 Lock 1개 방식 불허**
- **Listener 1개**: 연결 수락 + 수신 감시 + 파싱 + 큐 적재. `select/poll/epoll/NIO(selectors)` 등 다중 소켓 감시 사용. 좌석 직접 처리 X, 연결마다 스레드 X
- **Worker 10개(고정)**: §3 규칙대로 처리 후 응답. 소켓별 송신 Lock 등으로 응답 뒤섞임 방지
- RESERVE_MULTI: 오름차순 Lock → 전부 확인 → 일괄 반영·해제
- 바인딩 IP/포트: 실행 인자 또는 설정 파일
- **5초마다 POOL 로그**: 전체 좌석 현황, Request Queue 현재 길이, 누적 처리량 (Listener 감시 타임아웃 or Monitor 스레드)
- 모든 이벤트 Server.txt에 wall-clock으로 기록, **Graceful Termination**
- Worker 대기·깨우기는 **Condition Variable** (불가 시 Readme에 사유 설명 후 임의 구현 가능하나 **감점**). Waitlist 통지도 CV 기반(전용 Notifier 등)으로, **Worker는 Waitlist 때문에 블로킹 금지**

**조가 자유롭게 결정 (Readme에 설명)**
- Request Queue 크기 / 가득 찼을 때 동작 / 구현 방식 (예: Listener가 자리가 날 때까지 대기)
- 좌석·Waitlist 자료구조, Lock 세부 방식, 취소 시 넘기는 방법 (단 **FIFO + 끼어들기 없음** 만족)
- NOTIFY 전송 방식 (전용 Notifier 권장)
- 메시지 형식, 이중예약 검증 방법, Lock 경합 횟수 측정 방식
- 프로그래밍 언어 자유 (C/C++ pthread, Java ReentrantLock·Condition, Python threading.Lock·Condition 등)

**권장**
- Notifier Thread + Notify Queue(Mutex+CV) 구조
- Lock 잡은 채 I/O 하지 않기 (전송·파일 로그는 Lock 해제 후)
- 여러 Lock을 동시에 잡는 경우는 RESERVE_MULTI뿐이 되도록 설계

### 5-2. Client (30개 동시)

**필수**
- 독립 프로세스 또는 스레드로 로컬 실행, Server와 **TCP 연결 1개 유지**
- RESERVE / RESERVE_MULTI(2~4석) / CANCEL 중 무작위 선택, **0.2~1.0초 무작위 간격**
- Client당 **5,000건** (총 150,000건)
- **응답을 기다리지 않고** 다음 요청 계속 전송. 응답·NOTIFY는 별도 수신 스레드 등으로 받음
- 충돌 유도: 요청 좌석의 **절반 이상을 일부 좌석(예: 1~10번)** 에 집중
- CANCEL은 자신이 보유한 좌석(SUCCESS 또는 NOTIFY로 받은 좌석) 중 선택. 보유 좌석 없으면 대신 예약 요청
- '처리 완료' = 첫 응답(SUCCESS/FAIL/WAITLISTED) 수신 시점. NOTIFY는 별도 이벤트로 기록
- 모든 요청/응답을 `ClientN.txt`에 기록 (요청 시각, 요청 내용, 응답 시각, 결과)
- 5,000건 전송 + 모든 첫 응답 수신 후 **Server 종료 신호까지 연결 유지**(그 사이 NOTIFY도 기록) → 종료 신호 받으면 **최종 보유 좌석 목록** 기록 후 종료

**자유 (참고 권장안)**

| 내 상태 | RESERVE | RESERVE_MULTI | CANCEL |
|---|---|---|---|
| 보유 좌석 없음 | 60% | 40% | - |
| 보유 좌석 있음 | 30% | 20% | 50% |

- 인기 좌석 범위·비율, 다중 예약 좌석 수, 보유 좌석 많을 때 취소 정책(설명 슬라이드 예: 5개 이상이면 CANCEL 우선) 등
- 개발 중엔 요청 수/간격 줄이는 실행 인자 OK (예: `--requests 200`). **제출용 최종 실행은 반드시 30 × 5,000건, 0.2~1.0초**

### 5-3. 통신 프로토콜 (형식 자유, 조건 필수)
1. **메시지 경계**: TCP는 바이트 스트림 → `\n` 구분 또는 길이 접두 등. 한 번의 recv에 여러 메시지/한 메시지가 쪼개져 와도 처리 (버퍼에 모았다가 한 줄씩 꺼내기)
2. **요청 번호**: Client는 접속 시 자기 번호를 알리고, 요청마다 Client별 요청 번호. 응답·NOTIFY에 그 번호 포함
3. **종료 신호**: Server는 종료 시 모든 Client에게 종료 신호

예시 (그대로 써도 됨, 한 줄 = 한 메시지):

```
Client → Server            Server → Client
HELLO 9                    RESP 812 WAITLISTED
RESERVE 812 1              NOTIFY 812 1
RESERVE_MULTI 310 5,3      BYE
CANCEL 455 1
```

### 5-4. 그 외 조가 정하는 사항 (Readme에 선택 내용과 이유)
- Thread Pool·Request Queue 구현 방식 (직접 or `ExecutorService`/`ThreadPoolExecutor`/`BlockingQueue` 등. 라이브러리 사용 시 어느 부분이 Request Queue이고 길이를 어떻게 측정했는지 설명)
- 이중예약 확인 근거 (어떤 로그, 어떤 방법), Client 최종 보유 좌석 로그 형식
- 집계 기준 (예: handoff를 "해제 1 + 배정 1"로 셀지)
- Client 내부 상태 관리 (응답 미도착 좌석, 이미 취소 보낸 좌석 처리)
- 실행 안정성 (연결 끊김 처리, 재접속/재실행 여부, 로그 용량 관리·압축)
- 시연 방법 (영상 구성, 시연용 시나리오)
- 시간대 표기 (UTC/KST)

---

## 6. 필수 조건 (하나라도 미충족 → 과제 전체 0점) & 감점

### ❌ 0점 조건
1. 모든 통신은 **Socket(TCP)**
2. Server는 **별도 물리적 원격 호스트** 에서 실행, 최종 로그가 이 구성에서 나온 것
3. **Worker 10개 고정 Thread Pool + Request Queue** (요청/연결마다 스레드 생성 시 0점)
4. 좌석 정보 모든 접근은 **좌석별 Mutex/Lock** 으로 보호
5. 부하 테스트(30 × 5,000) 후 **이중예약 0건** (1건이라도 0점)
6. RESERVE_MULTI **원자적(All-or-Nothing)** + **오름차순 Lock Ordering**, 테스트 중 Deadlock으로 서버 멈추면 0점
7. (Readme 항목) **컴파일·실행 불가 시 0점**, **압축 파일 오류 시 0점**

### △ 주요 감점
- Waitlist 좌석 배정이 등록 순서(FIFO)를 지키지 않음
- Worker 대기·깨우기를 CV로 구현하지 않음 (busy-waiting 포함)
- Worker가 Waitlist 대기 때문에 블로킹됨
- Deadlock을 Lock Ordering이 아닌 타임아웃 후 강제 종료 등 임시방편으로 회피
- Readme 필수 항목 누락 또는 실제 구현과 다름
- 시연 영상 링크 오류·공유 권한 없음·재생 오류

### 버그 잘 나는 곳 TOP 5 (설명 슬라이드)
1. 확인과 변경을 Lock 밖에서 → 그 사이 뺏김
2. 다중 예약 도중 실패 return 시 Lock 미해제
3. Worker와 Notifier가 같은 소켓에 동시 write → 소켓별 송신 Lock
4. Queue Lock + 좌석 Lock 중첩 → 가능하면 하나씩, 중첩 시 순서 고정
5. 종료 시 CV를 깨우지 않으면 join이 영원히 안 끝남

---

## 7. 시뮬레이션 시나리오 (Step 1~7)

| Step | 내용 |
|---|---|
| 1. 서버 초기화 | 원격에서 좌석 100석 EMPTY, Worker 10개(+Notifier) 생성, Listener 연결 수락 시작 |
| 2. 다중 Client 접속 | 로컬에서 30개 순차/동시 접속, 접속 직후부터 무작위 요청 생성 |
| 3. 동시 단일 예약 | 인기 좌석 동시 요청 → Lock으로 1명에게만 배정, 나머지 WAITLISTED/FAIL |
| 4. 다중 예약 & Deadlock 회피 | [5,3] vs [3,5] 같은 겹치는 조합 → Lock Ordering으로 처리 (전형적: 하나 성공, 하나 전체 실패) |
| 5. 취소 & Waitlist 통지 | 대기자 없으면 EMPTY, 있으면 대기 1번에게 즉시 배정, Notifier가 CV로 깨어나 NOTIFY |
| 6. 부하 테스트 | 30 × 5,000 = 150,000건, 약 **50분**. 5초마다 POOL 로그. **마감 전 리허설 + 최종 실행 시간 확보** |
| 7. 종료 & 정합성 검증 | 아래 참조 |

**Step 7 상세**
- 첫 응답 전송 누적 **150,000건** 이 되면 → **Notify Queue에 남은 통지 전송** → 종료 절차 시작
- Graceful Termination: 모든 Client에 종료 신호 → 최종 좌석 현황·이중예약 검사 결과·전체 통계를 Server.txt에 기록 → 소켓 닫기 → **모든 스레드 join** → 로그 파일 정상 close
- 종료 시 Waitlist에 남은 요청은 오류 아님 → **"미해결 대기 N건"** 으로 기록
- §8-2 방법으로 이중예약 0건 및 정합성 확인 후 기록

---

## 8. 성능 지표 & 정합성

### 8-1. 측정 지표 (Server.txt / ClientN.txt 최종 로그 + Readme)

| 지표 | 단위 | 정의 (측정 위치) |
|---|---|---|
| 전체 처리량 | req/s | [Server] 첫 응답 보낸 요청 수 ÷ 실행 시간 (**첫 Client 연결 ~ 마지막 요청의 첫 응답 전송**) |
| 평균 응답 시간 | ms | [Client 시계] 요청 전송 ~ 첫 응답 수신 평균 (네트워크 지연 포함) |
| Request Queue 최대 길이 | 건 | [Server] 실행 중 큐에 쌓인 요청 수 최댓값 |
| 이중예약 발생 건수 | 건 | [Server] 동일 좌석 2명 이상 동시 배정 사례 수 (**반드시 0**) |
| Deadlock 발생 횟수 | 건 | [Server] 큐에 요청이 있는데 일정 시간(예: 30초) 이상 처리가 진행되지 않은 횟수. 간단한 감시로 충분 |
| Waitlist 평균 대기시간 | sec | [Server 시계] Waitlist 등록 ~ 해당 요청 NOTIFY 전송 평균 (미해결 제외) |
| Lock 경합 횟수 | 건 | [Server] Worker가 좌석 Lock을 얻기 위해 대기한 총 횟수. 측정 방식(예: try-lock 실패 횟수) Readme에 명시 |
| 최종 좌석 정합성 | PASS/FAIL | §8-2 항목 모두 만족 시 PASS |

### 8-2. 이중예약 & 최종 정합성 확인 (방법 자유, 별도 검증 프로그램 제출 불필요)
최소 다음 3가지 확인 후 기록:
1. **배정·해제 균형**: `(배정 수 − 해제 수) = 종료 시점 예약된 좌석 수`
2. **Server ↔ Client 대조**: Server 최종 좌석 현황 = 30개 Client의 최종 보유 좌석 목록 (좌석 수 합, 좌석별 owner)
3. **Waitlist 수지**: `전체 WAITLISTED = 전체 NOTIFY 수신 + 종료 시 미해결 대기`

모두 만족 + 이중예약 0건 → **"최종 좌석 정합성 = PASS"**

### 8-3. 결과 예시 (참고용, 제출 시 실측값으로 채울 것)

| Server 지표 | 예시 |
|---|---|
| Throughput | 49.6 req/s |
| Queue 최대 길이 | 6건 |
| 이중예약 | 0건 |
| Deadlock | 0건 |
| Waitlist 평균 대기 | 14.3 sec |
| Lock 경합 | 1,840건 |
| 정합성 | PASS |

| Client 합계 | 예시 |
|---|---|
| 총 요청 | 150,000 |
| SUCCESS | 71,930 (48.0%) |
| FAIL | 61,420 (40.9%) |
| WAITLISTED | 16,650 (11.1%) |
| NOTIFY 수신 / 미해결 | 16,470 / 180 (합 = 16,650) |
| 평균 응답시간 | 62 ms |

개별 Client 예 (Client9): SUCCESS 2,395 · FAIL 2,050 · WAITLISTED 555, 평균 61ms, NOTIFY 548, 미해결 7. 좌석#1 Waitlist 대기 0.400초(서버 기준 09.810 → 10.210) — '평균 응답 시간'과는 별개 지표.

---

## 9. 로그 형식

```
[HH:MM:SS.mmm] NODE | EVENT | STATUS | message
```

- **EVENT**: `INIT, CONNECT, RESERVE, RESERVE_MULTI, CANCEL, LOCK, WAITLIST, NOTIFY, POOL, DOUBLE_BOOKING_CHECK, TERMINATE`
- **STATUS**: `SUCCESS, FAIL, INFO, WARN`
- `LOCK` 로그 = RESERVE_MULTI 처리에서 **Lock 획득 순서** 기록용
- message 세부 내용은 조가 정하고 **AllDefinedLogs.txt** 에 정리

Server.txt 예시 (명세 발췌):
```
[14:32:00.000] SERVER | INIT | SUCCESS | Seat map(100) initialized. Worker pool size=10.
[14:32:00.510] SERVER | CONNECT | SUCCESS | Client1~Client30 connected (30/30).
[14:32:01.000] SERVER | RESERVE | INFO | Worker#3 Client5 req=101 seat#42.
[14:32:01.001] SERVER | RESERVE | SUCCESS | Worker#3 acquired lock(seat#42) first -> seat#42 assigned to Client5.
[14:32:01.002] SERVER | WAITLIST | SUCCESS | Worker#7 seat#42 already taken by Client5 -> Client12 registered (pos=1).
[14:32:03.500] SERVER | RESERVE_MULTI | INFO | Worker#2 Client7 req=310 seats[5,3]. Lock order -> [3,5].
[14:32:03.501] SERVER | LOCK | SUCCESS | Worker#2 acquired seat#3 -> seat#5 (ascending). No deadlock.
[14:32:03.501] SERVER | LOCK | INFO | Worker#9 waited for lock(seat#3) (contention).
[14:32:03.502] SERVER | RESERVE_MULTI | SUCCESS | Client7 seats[3,5] assigned (atomic).
[14:32:03.502] SERVER | RESERVE_MULTI | FAIL | Worker#9 Client21 rejected: seats already taken (all-or-nothing, nothing changed).
[14:32:05.000] SERVER | POOL | INFO | queue=1 max_queue=3 processed=248 waitlist_total=4 contention=17.
[14:32:10.200] SERVER | CANCEL | SUCCESS | Worker#4 Client2 req=455 canceled seat#1. Assigned to waitlist head Client9.
[14:32:10.210] SERVER | NOTIFY | SUCCESS | Notifier sent NOTIFY to Client9 req=812 seat#1 (waited 0.400s).
[15:22:24.000] SERVER | DOUBLE_BOOKING_CHECK | SUCCESS | 0 double-booking. assigned=41,213 released=41,116 reserved_now=97.
[15:22:24.020] SERVER | TERMINATE | INFO | pending_waitlist=180. Final seat map logged.
[15:22:24.050] SERVER | TERMINATE | SUCCESS | Graceful shutdown. Termination signal sent to 30 clients, all threads joined.
```

Client9.txt 예시:
```
[14:32:09.775] CLIENT9 | RESERVE | INFO | req=812 sent seat#1.
[14:32:09.845] CLIENT9 | WAITLIST | WARN | req=812 WAITLISTED seat#1. resp_time=70ms.
[14:32:10.245] CLIENT9 | NOTIFY | SUCCESS | req=812 seat#1 assigned from waitlist.
[15:22:24.080] CLIENT9 | TERMINATE | SUCCESS | Termination signal received. sent=5000 responded=5000 final_held=[1,17,42].
```

---

## 10. 제출물 체크리스트 (`G조이름HW2.zip`)

- [ ] **전체 소스 코드** (Server, Client)
- [ ] **AllDefinedLogs.txt** — INIT, CONNECT, RESERVE, RESERVE_MULTI, CANCEL, LOCK, WAITLIST, NOTIFY, POOL, DOUBLE_BOOKING_CHECK, TERMINATE 등 정의한 모든 로그 메시지 명세·설명
- [ ] **Server.txt, Client1.txt ~ Client30.txt** — 원격 구성 정식 부하 테스트(30 × 5,000) **1회분**, Server와 Client 로그가 **같은 실행** 에서 나온 것
- [ ] **download.txt** — 5분 이내 시연 영상(`G조이름HW2.mp4`) 다운로드 링크 (링크/권한/재생 오류 시 감점)
  - 영상 구성 자유. 예: ① 원격 Server 실행 + 로컬 Client 30개 접속·부하 시작 ② 다중 좌석 동시 예약 장면 ③ 이중예약 0건 확인 장면
- [ ] **Readme.txt** — 아래 모두 포함
  - [ ] 조원 이름·학번·역할
  - [ ] 배포 정보: 원격 호스트(서비스명, 인스턴스 유형, OS), Server/Client 실행 명령, **사용 시간대** (공인 IP는 가려도 됨)
  - [ ] 프로그램 구성요소 설명(Server, Client 역할) 및 Client–Server 메시지 형식
  - [ ] 컴파일·실행 방법 — Server IP/포트 설정, Client 서버 주소 지정 방법 (**실행 불가 시 0점**)
  - [ ] Thread Pool 설계 (Request Queue 구조·크기·가득 찼을 때 동작, **장단점 분석**)
  - [ ] 동시성 제어(Mutex/Lock) 전략 — **Critical Section 범위와 근거 명시 필수**
  - [ ] Lock Ordering Deadlock 회피 알고리즘 (**장단점 분석**)
  - [ ] Waitlist / CV / (Notifier) 구현 방식 (**FIFO 보장 방법**, CV 미사용 시 이유와 대체 방식)
  - [ ] Race Condition·이중예약 방지 테스트 방법·결과, §8-2 정합성 확인 방법·결과 (**§8-3 형식 실측 결과표**)
  - [ ] 조가 자유롭게 정한 사항(§5-4 등)의 선택 내용과 이유
  - [ ] Lock 경합 측정 방식
  - [ ] 추가 구현 사항 및 기타

### 추천 개발 순서 (설명 슬라이드)
1. 소켓 기본 — Client 1개 ↔ Server 한 줄 주고받기, 줄바꿈 단위 분리
2. Listener + Queue + Pool — 30개 접속을 한 스레드로 감시 → 큐 → Worker 10개 에코
3. 좌석 + Lock (단일 RESERVE/CANCEL), 같은 좌석 동시 요청 테스트
4. 다중 예약 — 정렬 후 오름차순 Lock, [5,3] vs [3,5] 테스트
5. Waitlist + Notifier — WAITLISTED, CANCEL 시 대기 1번 배정, CV로 NOTIFY
6. 로그·통계·종료 — wall-clock 로그, POOL, 지표, 종료 신호, Graceful Termination
7. 원격 배포 → 소규모 테스트 → 최종 30×5,000(약 50분) → 정합성 확인 → 영상

---

## 11. 두 문서 간 차이점 / 불일치 체크

> 기본 원칙: **description(명세)이 공식 문서**, explanation(슬라이드)은 요약·보충. 충돌 시 **두 쪽을 모두 만족하는 더 엄격한 쪽** 으로 구현하는 게 안전.

### 🔴 실제로 내용이 충돌하는 부분

| # | 주제 | description (명세) | explanation (슬라이드) | 권장 대응 |
|---|---|---|---|---|
| 1 | **Notifier Thread가 필수인가** | 모든 언급에 "(권장)" 표기 (p.2, p.3 구조도·스레드 규칙, p.5, p.9, p.10, p.13 Step 1·5). 단 "Waitlist 통지도 CV 기반(전용 Notifier 등)으로 처리" (p.10 박스) | **p.18** "필수(모든 조 공통)" 목록에 "Waitlist 통지는 Condition Variable + Notifier Thread" 포함, **p.23** Step 1·5도 "(권장)" 없이 Notifier 생성/동작 기술, **p.8** 구조도도 "Notifier (1)"로만 표기하고 **Notify Queue는 그림에서 빠짐**(명세 구조도엔 `Notify Queue (Mutex + CondVar)` 있음) ↔ **p.7** "Notifier 1개(권장)", **p.17** "Notifier는 권장일 뿐 필수 아님 — 안 쓰면 Worker가 락 해제 직후 직접 통지해도 됨" (슬라이드 내부에서도 모순) | **Notifier Thread + Notify Queue(Mutex+CV)로 구현** → 어느 해석으로도 안전 |
| 2 | **Notifier 없이 Worker가 직접 통지해도 되나** | Notifier 안 써도 "위의 CV 규칙은 동일하게 적용", "Waitlist 통지도 CV 기반" | p.17 "Worker가 락 해제 직후 직접 통지해도 됩니다" (CV 언급 없음) | 1번과 동일하게 Notifier+CV 사용 |
| 3 | **범위 검사를 Lock 안에서 하나** | 범위 밖/개수/중복 오류는 **Lock 잡기 전에** FAIL, 그 외만 Lock 안에서 판정 (p.3, p.6) | p.11 표 제목: "좌석 락을 잡은 상태에서 위에서부터 순서대로 판정" — 범위 검사 행도 포함된 것처럼 읽힘 (p.15 MULTI는 "락 잡기 전에"로 명세와 일치) | **범위 검사는 Lock 전** (범위 밖 번호로 Lock 배열 접근하면 인덱스 오류도 남) |
| 4 | **CANCEL 범위 검사** | CANCEL도 "1~100 범위 밖 → FAIL" 행 있음 | p.11 표의 CANCEL에 범위 밖 행이 **누락** | CANCEL도 범위 검사 구현 |
| 5 | **FIFO 감점 기준 표현** | "Waitlist의 **좌석 배정**이 등록 순서(FIFO)를 지키지 않는 경우", 기준은 **NOTIFY 도착 순서가 아니라 좌석 배정 순서** (p.5, p.12) | p.22 "Waitlist **통지**가 좌석별 FIFO 순서를 어기는 경우" | 명세 기준: **Lock 안에서 배정하는 순서**가 FIFO이면 됨. NOTIFY 전송 순서가 뒤바뀌는 건 감점 아님 |

### 🟡 타이밍/예시 수치 차이 (구현 영향 없음)

| # | 내용 |
|---|---|
| 6 | Race Condition 예시 타임라인: 명세는 Worker#3 배정 01.001 → 해제·SUCCESS 01.002 → Worker#7 해제·WAITLISTED 01.003. 슬라이드 p.12는 Worker#3 확정·해제 1.002, Worker#7 Lock 획득·WAITLISTED 1.003, Mutex 미사용 쪽은 "둘 다 SUCCESS"를 1.001에 둠. 개념은 동일 |
| 7 | 로그 예시: 슬라이드 p.27은 명세 예시를 축약 (POOL에서 `waitlist_total` 빠짐, DOUBLE_BOOKING_CHECK에서 `reserved_now` 빠짐, TERMINATE INFO 줄 없음, req 번호·Worker 번호 일부 생략, Client WAITLIST 줄에 `resp_time` 없음). 또 **"Server.txt (발췌)" 박스 안에 CLIENT9 로그 2줄이 섞여 있음** — Client 로그는 `ClientN.txt`에 따로 남겨야 함. **명세 쪽 예시를 따르는 게 정보가 더 많음** |

### 🟠 슬라이드에서 빠진(축약된) 필수 정보 — 명세만 보고 챙겨야 함

| # | 빠진 내용 (명세에만 있음) |
|---|---|
| 8 | **Readme 필수 항목**: 슬라이드 p.28은 4줄 요약본 (조원·배포정보 / 구성요소·메시지·컴파일/실행 / 설계와 장단점 / 이중예약 확인·실측표·기타). 명세에만 있는 항목: **사용 시간대**, Request Queue **크기·가득 찼을 때 동작**, **Critical Section 범위와 근거(필수)**, **FIFO 보장 방법**, CV 미사용 시 이유·대체 방식, **Race Condition 테스트 방법·결과**, §4-2 **3가지 정합성 확인 결과**, **조가 자유롭게 정한 사항과 이유**, Lock 경합 측정 방식(§4-1) → §10 체크리스트 기준으로 작성 |
| 9 | **처리량 측정 구간**: 명세는 "첫 Client 연결 ~ 마지막 요청의 첫 응답 전송". 슬라이드 p.24엔 구간 없음 |
| 10 | **Step 7 종료 조건**: 명세는 "첫 응답 전송 누적 150,000건 → Notify Queue 잔여 통지 전송 → 종료 신호", 종료 시 **최종 좌석 현황·이중예약 검사·전체 통계**를 Server.txt에 기록, **로그 파일 정상 close**. 슬라이드는 축약 |
| 11 | **§1-④ 자유 결정 항목**(집계 기준, Client 내부 상태 관리, 연결 끊김/로그 용량/압축, 시간대 등)이 슬라이드엔 없음 |
| 12 | 시간대를 **Readme에 기재**하라는 요구가 슬라이드엔 없음 (슬라이드는 NTP 권장만) |
| 13 | AllDefinedLogs.txt에 **INIT도 포함**하라는 명세 문구 (슬라이드는 "정의한 모든 로그"로만 표현) |
| 16 | **Client가 접속 시 자기 번호를 알려야 함** (명세 §1-③ 필수 조건). 슬라이드 p.20 "꼭 지켜야 할 3가지"엔 요청 번호만 있고, 접속 시 번호 통지는 예시(`HELLO 9`)에만 나옴 |
| 17 | POOL 로그를 **어떤 스레드가 찍는지**: 명세는 "Listener의 감시 타임아웃 이용 또는 선택 Monitor Thread 1개". 슬라이드 p.18엔 방법이 없음 → 별도 타이머 스레드를 만들면 스레드 규칙 위반이니 주의 |
| 18 | Client 쪽 스레드는 **제한 없음** (명세 §1-②). 슬라이드엔 명시 없음 |
| 19 | FAIL 사유 표기 방식은 **자유** (명세 §0-3). 슬라이드엔 없음 |

### 🟡 강도(필수/권장) 표현 차이

| # | 내용 |
|---|---|
| 14 | "Lock을 동시에 2개 이상 잡는 건 RESERVE_MULTI뿐": 명세 p.10은 **권장**("그렇게 설계하면 쉽다"), 슬라이드 p.13은 "락 사용 **3원칙**"으로 규칙처럼 표현. 감점 조항은 아님 |
| 15 | "Lock 잡은 채 I/O 금지": 명세는 **권장**. 슬라이드 p.10은 "권장사항 위반 · **공식 감점 사유는 아님**"이라고 명시해 줌 (명세의 감점 목록에도 없음 → 일치) |
| 20 | **Worker가 Waitlist 때문에 블로킹**: 명세 §2는 **감점** 사유. 슬라이드 p.17은 "Worker가 좌석이 빌 때까지 CV에서 잠들기 → Worker 10개가 모두 잠들면 서버 정지 (**0점 위험**)". 모순이라기보다는, 블로킹 자체는 감점이고 그 결과 서버가 멈추면 0점 조건("테스트 중 서버 멈춤")에 걸린다는 뜻 → **절대 하지 말 것** |
| 21 | **원격 호스트의 의미**: 명세는 "별도의 **물리적** 원격 호스트(AWS, GCP 등)", 슬라이드 p.8·9는 "클라우드 **VM**". 둘 다 AWS/GCP 무료·저가 인스턴스를 예로 드니 **로컬 PC와 물리적으로 다른 머신**이라는 뜻이고, 클라우드 VM이면 충분함 (로컬 VM/WSL/Docker는 불인정으로 보는 게 안전) |
| 22 | 슬라이드 p.6 "Condition Variable / Waitlist: 이미 예약된 좌석은 대기열에 등록"은 **단일 RESERVE만** 해당된다는 조건이 빠짐. MULTI는 대기열 등록 안 함 (p.11·15·30에서는 정확히 기술) |

### 🟢 슬라이드에만 있는 보충 정보 (명세와 충돌 X, 참고용)
- 락을 쥔 채 I/O 하는 건 "권장사항 위반이지만 **공식 감점 사유는 아님**" (p.10)
- 버그 TOP 5 (Queue Lock + 좌석 Lock 중첩 시 순서 고정, 종료 시 CV 깨우기 등) (p.22)
- 보유 좌석 5개 이상이면 CANCEL 우선 팁, `--requests 200` 같은 개발용 인자 예시 (p.19)
- Azure도 가능, 비용은 조 책임, NTP 권장, 응답시간 60ms대 정상 (p.9)
- 영상 구성 예시 3장면 (p.28), 추천 개발 순서 (p.29), FAQ (p.30) — Queue 가득 차면 예: "Listener가 자리 날 때까지 대기"
- Client 로그 STATUS 의미: INFO=정보성, WARN=대기 상태 주의 (p.27)

### ⚠️ 명세 자체에서 애매한 점
- **POOL 로그 내용**: 요구사항은 "전체 좌석 현황 + Queue 현재 길이 + 누적 처리량"인데, 두 문서의 POOL 로그 예시 모두 **좌석 현황 항목이 없음** (`queue, max_queue, processed, waitlist_total, contention`만). → 안전하게 `reserved=NN/100` 같은 좌석 현황 요약을 POOL 줄에 추가하고, 전체 좌석 맵은 종료 시 출력 권장
- **Deadlock 감시 기준 시간**: "일정 시간(예: 30초)" — 값은 조가 정해 Readme에 명시. 감시는 Listener의 select 타임아웃이나 선택 Monitor 스레드로 해야 함 (스레드 추가 금지 규칙 때문)
- **서버의 종료 시점 판단**: 서버는 "첫 응답 누적 150,000건"이 되면 종료해야 하므로, **총 요청 수(30 × 5,000)를 서버도 알아야** 함. 개발 중 요청 수를 줄이는 인자를 쓰면 서버 쪽에도 같은 값을 넘겨야 함 (실행 인자/설정 파일로 받기 권장, HELLO에 요청 수를 실어 보내는 것도 방법)
- **배정/해제 집계 기준(handoff)**: 명세는 "handoff를 해제 1 + 배정 1로 셀지"를 조가 정하라고 함. 어느 쪽이든 `배정 − 해제 = 종료 시 예약 좌석 수`가 성립하도록 일관되게 세고 Readme에 명시

---

## 12. 2차 교차검증 메모

### 12-1. 1차 정리에서 바로잡은 것
- 1차 정리 §11-8에서 "슬라이드에 **장단점 분석**, **실행 명령**이 빠졌다"고 썼는데, 이는 **틀린 내용**이었다. 슬라이드 p.28에도 "설계와 장단점", "컴파일/실행 방법(IP/포트 지정 포함)"이 있다. 실제로 슬라이드에서 빠진 항목만 남기도록 고쳤다.
- §11-1 Notifier 항목에 근거 페이지를 보강했다 (슬라이드 p.23 Step 1·5에도 "(권장)" 표기가 없음).

### 12-2. 예시 수치 검산 (명세 §4-3, §5)

| 검산 | 결과 |
|---|---|
| SUCCESS + FAIL + WAITLISTED = 71,930 + 61,420 + 16,650 | = 150,000 ✅ |
| 비율 48.0% / 40.9% / 11.1% | ✅ |
| NOTIFY 16,470 + 미해결 180 = WAITLISTED 16,650 | ✅ |
| Client9: 2,395 + 2,050 + 555 = 5,000, NOTIFY 548 + 미해결 7 = 555 | ✅ |
| DOUBLE_BOOKING_CHECK: assigned 41,213 − released 41,116 = reserved_now 97 | ✅ |
| Throughput: 14:32:00 → 15:22:24 = 3,024초, 150,000 ÷ 3,024 ≈ 49.6 req/s | ✅ |
| 30명 ÷ 평균 0.6초 = 50 req/s, 5,000 × 0.6초 = 50분 | ✅ |
| Client9 Waitlist 대기: 서버 09.810 → 10.210 = 0.400s, 클라 응답 09.775 → 09.845 = 70ms | ✅ |
| Race 예시 타임라인: 명세 §0-4 ↔ §0-7 예제 1 | 서로 일치 ✅ (슬라이드 p.12만 1ms씩 다름) |
| assigned/released vs SUCCESS 수 | ⚠️ 아래 참고 |

⚠️ **assigned 41,213 / released 41,116과 SUCCESS 71,930을 함께 보면, handoff를 "해제 1 + 배정 1"로 세는 방식에서는 성립하지 않는다.**
- 그 방식이면 CANCEL 성공 = released = 41,116이다. 그러면 RESERVE·MULTI 성공 응답 = 30,814건이다.
- 그런데 assigned − handoff(16,470) = 24,743석뿐이라, 응답 수보다 배정 좌석 수가 적어진다. 이는 불가능하다.
- handoff를 집계에서 빼는 방식이면 수학적으로는 가능하다. 다만 MULTI 성공이 단일 RESERVE 성공보다 훨씬 많아야 해서 다소 부자연스럽다.

→ 명세가 "수치는 예시일 뿐"이라고 했으므로 **우리 결과가 이 예시 수치와 비슷한지로 정상 여부를 판단하면 안 된다.** 우리 집계 기준으로 §8-2 세 가지 등식만 맞추면 된다.

### 12-4. 3차 교차검증에서 추가한 것
- 슬라이드 p.8·19·27을 이미지로 추가 확인. 새로 찾은 항목: §11-1 보강(p.8 구조도에 Notify Queue 누락), §11-7 보강(Server.txt 예시 박스에 Client 로그 혼입), §11-16~22 신규
- 이번 정리 문서(§0~§10)의 주요 문장을 원문 위치와 다시 대조함. 사실 오류는 없었음. §9 로그 예시는 명세 예시 중 일부 줄(RESERVE INFO 2줄, MULTI INFO 1줄, 09.810 WAITLIST 1줄)을 생략한 발췌임

### 12-3. 두 문서에서 일치 확인된 핵심 규칙 (안심하고 따라도 되는 것)
- Worker 10개 고정 / Listener 1 / Notifier 1(권장) / Monitor 1(선택), 그 외 스레드 금지
- 좌석별 Mutex(100개), 전체 Lock 1개 불가
- §3 판정표의 RESERVE·RESERVE_MULTI 결과값 (CANCEL 범위 검사만 슬라이드 누락)
- MULTI = 오름차순 Lock + All-or-Nothing + Waitlist 미등록
- CANCEL 시 대기 1번에게 Lock 안에서 즉시 배정(끼어들기 불가), FIFO
- Worker는 큐 대기에 CV 사용, Waitlist 때문에 블로킹 금지, busy-waiting 감점
- 0점 조건 6개 (문구 동일)
- 지표 8개 정의, 정합성 3가지 확인, 로그 형식·EVENT·STATUS 목록, 제출물 4종 + Readme
