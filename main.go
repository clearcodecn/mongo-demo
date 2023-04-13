package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	option := options.Client().ApplyURI("mongodb://192.168.1.101:27017")
	option.SetAuth(options.Credential{
		AuthMechanism:           "SCRAM-SHA-256",
		AuthMechanismProperties: nil,
		AuthSource:              "admin",
		Username:                "root",
		Password:                "tomAndJerryHouse",
		PasswordSet:             true,
	})

	client, err := mongo.Connect(ctx,
		option,
	)

	if err != nil {
		log.Fatal(err)
	}

	if err := client.Ping(context.Background(), &readpref.ReadPref{}); err != nil {
		log.Fatal(err)
	}

	//for i := 0; i < 10; i++ {
	//	user := &User{
	//		Id:         uuid.New().String(),
	//		Username:   fmt.Sprintf("user%d", i),
	//		Password:   fmt.Sprintf("user%d", i),
	//		Tags:       []string{fmt.Sprintf("tag%d", i)},
	//		CreateTime: time.Now(),
	//	}
	//	result, err := client.Database("test").Collection("users").InsertOne(context.Background(), user)
	//	if err != nil {
	//		log.Fatal(err)
	//		return
	//	}
	//	id := result.InsertedID.(string)
	//	fmt.Println("insert -> ", id)
	//}

	coll := client.Database("test").Collection("users")

	{

		query := bson.M{
			"username": bson.M{
				"$gt": "",
			},
		}

		opt := options.Find().SetLimit(2)

		page := 1
		pageSize := 2
		opt.SetSkip(int64((page - 1) * pageSize))
		opt.SetSort(bson.M{"_id": 1})

		cursor, err := coll.Find(context.Background(), query, opt)
		if err != nil {
			return
		}
		var users []User

		err = cursor.All(context.Background(), &users)
		if err != nil {
			return
		}
		fmt.Println(users)
	}

	{
		// update . upsert .
		query := bson.M{"_id": "162d5d88-449d-4615-9df9-c36769e63674"}

		update := bson.M{
			"$set": bson.M{
				"username": "update_username",
			},
		}

		coll.UpdateOne(context.Background(), query, update)
	}

	{
		i := 12
		user := &User{
			Id:         uuid.New().String(),
			Username:   fmt.Sprintf("user%d", i),
			Password:   fmt.Sprintf("user%d", i),
			Tags:       []string{fmt.Sprintf("tag%d", i)},
			CreateTime: time.Now(),
		}
		query := bson.M{"username": user.Username}
		update := bson.M{
			"$set": bson.M{
				"username":   user.Username,
				"password":   user.Password,
				"tags":       user.Tags,
				"createTime": user.CreateTime,
			},
			"$setOnInsert": bson.M{
				"_id": user.Id,
			},
		}
		opt := options.Update().SetUpsert(true)
		_, err := coll.UpdateOne(context.Background(), query, update, opt)
		if err != nil {
			log.Fatal(err)
		}
	}

	{
		res, err := coll.DeleteOne(context.Background(), bson.M{
			"username": "user12",
		})
		if err != nil {
			return
		}
		fmt.Println(res.DeletedCount)
	}

	{
		c, err := coll.CountDocuments(context.Background(), bson.M{})
		if err != nil {
			return
		}
		fmt.Println(c)
	}

	{
		coll.DeleteMany(context.Background(), bson.M{})
	}

	{
		for i := 0; i < 5; i++ {
			user := &User{
				Id:         uuid.New().String(),
				Username:   fmt.Sprintf("user%d", i),
				Password:   fmt.Sprintf("user%d", i),
				Tags:       []string{fmt.Sprintf("tag%d", i)},
				CreateTime: time.Now(),
				Code:       "before",
			}
			coll.InsertOne(context.Background(), user)
		}
		for i := 0; i < 5; i++ {
			user := &User{
				Id:         uuid.New().String(),
				Username:   fmt.Sprintf("user%d", i),
				Password:   fmt.Sprintf("user%d", i),
				Tags:       []string{fmt.Sprintf("tag%d", i)},
				CreateTime: time.Now(),
				Code:       "after",
			}
			coll.InsertOne(context.Background(), user)
		}

		var pip = []bson.M{
			{
				"$match": bson.M{
					"createTime": bson.M{
						"$lt": time.Now(),
					},
				},
			},
			{
				"$group": bson.M{
					"_id": "$code",
					"count": bson.M{
						"$sum": 1,
					},
				},
			},
		}

		var aggRes []AggResult
		cursor, err := coll.Aggregate(context.Background(), pip)
		if err != nil {
			log.Fatal(err)
			return
		}
		cursor.All(context.Background(), &aggRes)

		fmt.Println(aggRes)
	}

}

type User struct {
	Id         string    `json:"id" bson:"_id"`
	Username   string    `json:"username" bson:"username"`
	Password   string    `json:"password" bson:"password"`
	Tags       []string  `json:"tags" bson:"tags"`
	Code       string    `json:"code" bson:"code"`
	CreateTime time.Time `json:"create_time" bson:"createTime"`
}

type AggResult struct {
	Id    string `bson:"_id"`
	Count int    `bson:"count"`
}
