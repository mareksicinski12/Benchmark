SELECT * FROM (
    SELECT * FROM (
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM (
                        SELECT * FROM (
                            SELECT * FROM (
                                SELECT * FROM (
                                    SELECT * FROM (
                                        SELECT * FROM (
                                            SELECT * FROM (
                                                SELECT * FROM (
                                                    SELECT * FROM (
                                                        SELECT * FROM (
                                                            SELECT Id, Title, LENGTH(Title) AS TitleLength
                                                            FROM posts
                                                            WHERE LENGTH(Title) > 50
                                                        ) AS T1
                                                    ) AS T2
                                                ) AS T3
                                            ) AS T4
                                        ) AS T5
                                    ) AS T6
                                ) AS T7
                            ) AS T8
                        ) AS T9
                    ) AS T10
                ) AS T11
            ) AS T12
        ) AS T13
    ) AS T14
) AS T15
