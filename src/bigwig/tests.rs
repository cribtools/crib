use super::*;

struct BigWigMockData {
    data: Vec<BBIIter>,
    expected_output: String,
}

impl BigWigMockData {
    fn new(data: Vec<Vec<(u32, u32, f32)>>, output: String) -> BigWigMockData {
        BigWigMockData {
            data: data.into_iter().map(|data| data.into()).collect(),
            expected_output: output,
        }
    }
}

fn setup_valid() -> Vec<BigWigMockData> {
    let bigwig_data = vec![
        BigWigMockData::new(
            vec![
                vec![(1, 10, 1.0), (20, 30, 2.0)],
                vec![(2, 11, 3.0), (19, 30, 4.0)],
                vec![(2, 11, 5.0), (20, 30, 6.0)],
            ],
"1	1	2	1	.	.
1	2	10	1	3	5
1	10	11	.	3	5
1	19	20	.	4	.
1	20	30	2	4	6
"
            .to_string(),
        ),
        BigWigMockData::new(
            vec![vec![(2, 11, 5.0), (40, 50, 6.0)], vec![(10, 30, 7.0)]],
"1	2	10	5	.
1	10	11	5	7
1	11	30	.	7
1	40	50	6	.
"
            .to_string(),
        ),
        BigWigMockData::new(
            vec![
                vec![(2, 11, 5.0), (12, 50, 6.0)],
                vec![(10, 11, 7.0), (11, 12, 8.0)],
            ],
"1	2	10	5	.
1	10	11	5	7
1	11	12	.	8
1	12	50	6	.
"
            .to_string(),
        ),
        BigWigMockData::new(
            vec![
                vec![(1, 10, 1.0), (14, 30, 2.0)],
                vec![(5, 15, 3.0), (15, 20, 4.0)],
                vec![(2, 6, 5.0), (6, 15, 6.0)],
                vec![(5, 10, 5.0), (11, 30, 6.0)],
            ],
"1	1	2	1	.	.	.
1	2	5	1	.	5	.
1	5	6	1	3	5	5
1	6	10	1	3	6	5
1	10	11	.	3	6	.
1	11	14	.	3	6	6
1	14	15	2	3	6	6
1	15	20	2	4	.	6
1	20	30	2	.	.	6
"
            .to_string(),
        ),
    ];
    bigwig_data
}

fn setup_invalid() -> Vec<BigWigMockData> {
    let bigwig_data = vec![
        BigWigMockData::new(
            vec![
                vec![(2, 11, 1.0), (40, 50, 3.0)],
                vec![(10, 8, 2.0), (11, 12, 4.0)],
            ],
            String::new(),
        ),
        BigWigMockData::new(
            vec![
                vec![(2, 11, 5.0), (40, 50, 6.0)],
                vec![(10, 11, 7.0), (10, 12, 8.0)],
            ],
            String::new(),
        ),
        BigWigMockData::new(
            vec![
                vec![(2, 11, 5.0), (60, 50, 6.0)],
                vec![(10, 11, 7.0), (11, 12, 8.0)],
            ],
            String::new(),
        ),
    ];
    bigwig_data
}

#[test]
fn check_valid() {
    let test_data = setup_valid();
    let seqid: SeqId = "1".into();

    for (i, bigwig_mock) in test_data.into_iter().enumerate() {
        let mut data_sliders: Vec<_> = bigwig_mock
            .data
            .into_iter()
            .map(|mock| {
                let bw = BigWigSpawn::new(BigWigFile::Iter(mock), &seqid, 0, 0);
                DataSlide::new(bw).unwrap()
            })
            .collect();

        let mut buf = Vec::new();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .build().unwrap();
        runtime.block_on(data_slider_print(&mut buf, &seqid, &mut data_sliders)).unwrap();
        let result = String::from_utf8(buf).unwrap();
        println!("{}", result);
        assert!(
            result == bigwig_mock.expected_output,
            "output {i} doesn't match"
        );
    }
}

#[test]
fn check_invalid() {
    let test_data = setup_invalid();
    let seqid: SeqId = "1".into();

    for (i, bigwig_mock) in test_data.into_iter().enumerate() {
        let data_sliders: Result<Vec<_>, _> = bigwig_mock
            .data
            .into_iter()
            .map(|mock| {
                let bw = BigWigSpawn::new(BigWigFile::Iter(mock), &seqid, 0, 0);
                DataSlide::new(bw)
            })
            .collect();

        if let Err(CribError::InvalidDataFormat(_)) = data_sliders {
            continue;
        }
        let mut data_sliders = data_sliders.unwrap();

        let mut buf = Vec::new();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .build().unwrap();
        let result = runtime.block_on(data_slider_print(&mut buf, &seqid, &mut data_sliders));
        match result {
            Err(CribError::InvalidDataFormat(_)) => {}
            _ => {
                let result = String::from_utf8(buf).unwrap();
                println!("{}", result);
                panic!("output {i} should have failed with InvalidDataFormat.");
            }
        }
    }
}
