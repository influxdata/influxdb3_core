mod process_item_list {
    use futures::StreamExt as _;

    use object_store::{path::Path, ObjectMeta};

    use super::super::process_item_list;

    #[tokio::test]
    async fn preserves_icebergexporter_config_older_than_the_cutoff() {
        let items = vec![
            Ok(ObjectMeta {
                location: Path::from_url_path("a").unwrap(),
                last_modified: chrono::Utc::now(),
                size: 1,
                e_tag: Some("a".to_owned()),
                version: Some("1".to_owned()),
            }),
            Ok(ObjectMeta {
                location: Path::from_url_path("iceberg/conf").unwrap(),
                last_modified: chrono::Utc::now(),
                size: 1,
                e_tag: Some("conf".to_owned()),
                version: Some("1".to_owned()),
            }),
            Ok(ObjectMeta {
                location: Path::from_url_path("b").unwrap(),
                last_modified: chrono::Utc::now(),
                size: 1,
                e_tag: Some("b".to_owned()),
                version: Some("1".to_owned()),
            }),
        ];
        let (tx, mut rx) = tokio::sync::mpsc::channel(items.len());

        process_item_list(items, &tx).await.unwrap();
        rx.close();

        assert_eq!(
            vec![
                Path::from_url_path("a").unwrap(),
                Path::from_url_path("b").unwrap()
            ],
            tokio_stream::wrappers::ReceiverStream::new(rx)
                .map(|item| item.location)
                .collect::<Vec<_>>()
                .await
        );
    }
}
