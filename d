[
  {
    $lookup: {
      from: "rich_messages",
      let: {
        chatId: "$id"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $eq: [
                { $toString: "$chat_id" },
                { $toString: "$$chatId" }
              ]
            }
          }
        },
        {
          $addFields: {
            msg_dt: {
              $convert: {
                input: "$created_time",
                to: "date",
                onError: null,
                onNull: null
              }
            }
          }
        },
        {
          $match: {
            msg_dt: { $ne: null }
          }
        },
        {
          $match: {
            $expr: {
              $gte: [
                "$msg_dt",
                {
                  $dateSubtract: {
                    startDate: "$$NOW",
                    unit: "day",
                    amount: 2
                  }
                }
              ]
            }
          }
        },
        {
          $sort: {
            msg_dt: 1,
            _id: 1
          }
        },
        {
          $setWindowFields: {
            sortBy: {
              msg_dt: 1,
              _id: 1
            },
            output: {
              turn_no: {
                $sum: {
                  $cond: [
                    { $eq: ["$role", "user"] },
                    1,
                    0
                  ]
                },
                window: {
                  documents: ["unbounded", "current"]
                }
              }
            }
          }
        },
        {
          $addFields: {
            user_question_if_any: {
              $cond: [
                { $eq: ["$role", "user"] },
                {
                  $ifNull: [
                    "$content.text",
                    "$content"
                  ]
                },
                null
              ]
            },
            asked_at_if_any: {
              $cond: [
                { $eq: ["$role", "user"] },
                "$msg_dt",
                null
              ]
            },
            assistant_sql_if_any: {
              $cond: [
                { $eq: ["$role", "assistant"] },
                "$sql_response",
                null
              ]
            },
            assistant_text_if_any: {
              $cond: [
                { $eq: ["$role", "assistant"] },
                {
                  $ifNull: [
                    "$text_response",
                    "$content.text_response"
                  ]
                },
                null
              ]
            },
            assistant_md_if_any: {
              $cond: [
                { $eq: ["$role", "assistant"] },
                {
                  $ifNull: [
                    "$content.markdown_response",
                    "$markdown_response"
                  ]
                },
                null
              ]
            }
          }
        },
        {
          $group: {
            _id: "$turn_no",
            asked_at: { $first: "$asked_at_if_any" },
            question: { $first: "$user_question_if_any" },
            assistant_sqls: { $push: "$assistant_sql_if_any" },
            assistant_texts: { $push: "$assistant_text_if_any" },
            assistant_mds: { $push: "$assistant_md_if_any" }
          }
        },
        {
          $match: {
            question: { $ne: null }
          }
        },
        {
          $project: {
            _id: 0,
            asked_at: 1,
            question: 1,
            sql_response: {
              $reduce: {
                input: "$assistant_sqls",
                initialValue: null,
                in: {
                  $ifNull: ["$$value", "$$this"]
                }
              }
            },
            text_response: {
              $reduce: {
                input: "$assistant_texts",
                initialValue: null,
                in: {
                  $ifNull: ["$$value", "$$this"]
                }
              }
            },
            markdown_response: {
              $reduce: {
                input: "$assistant_mds",
                initialValue: null,
                in: {
                  $ifNull: ["$$value", "$$this"]
                }
              }
            },
            final_response: {
              $ifNull: [
                {
                  $reduce: {
                    input: "$assistant_mds",
                    initialValue: null,
                    in: {
                      $ifNull: ["$$value", "$$this"]
                    }
                  }
                },
                {
                  $ifNull: [
                    {
                      $reduce: {
                        input: "$assistant_texts",
                        initialValue: null,
                        in: {
                          $ifNull: ["$$value", "$$this"]
                        }
                      }
                    },
                    {
                      $reduce: {
                        input: "$assistant_sqls",
                        initialValue: null,
                        in: {
                          $ifNull: ["$$value", "$$this"]
                        }
                      }
                    }
                  ]
                }
              ]
            }
          }
        },
        {
          $sort: {
            asked_at: 1
          }
        }
      ],
      as: "user_questions_last2d"
    }
  },
  {
    $unwind: {
      path: "$user_questions_last2d",
      preserveNullAndEmptyArrays: false
    }
  },
  {
    $project: {
      _id: {
        $concat: [
          { $toString: "$id" },
          "_",
          {
            $dateToString: {
              format: "%Y-%m-%dT%H:%M:%S.%LZ",
              date: "$user_questions_last2d.asked_at"
            }
          }
        ]
      },
      user_id: 1,
      user_name: 1,
      user_email: 1,
      user_role: 1,
      project_id: 1,
      project_name: 1,
      chat_id: "$id",
      chat_name: {
        $ifNull: [
          "$chat_name",
          {
            $ifNull: [
              "$title",
              "$name"
            ]
          }
        ]
      },
      asked_at: "$user_questions_last2d.asked_at",
      question: "$user_questions_last2d.question",
      sql_response: "$user_questions_last2d.sql_response",
      text_response: "$user_questions_last2d.text_response",
      markdown_response: "$user_questions_last2d.markdown_response",
      final_response: "$user_questions_last2d.final_response"
    }
  },
  {
    $sort: {
      asked_at: -1
    }
  },
  {
    $out: "chat_question_answers_last2d"
  }
]
