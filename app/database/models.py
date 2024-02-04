from tortoise.models import Model
from tortoise import fields


class Post(Model):
    """Posts"""
    id = fields.BigIntField(pk=True)

    solved = fields.BooleanField()

    start_bytes = fields.BigIntField()
    length = fields.IntField()

    accepted_answer_id = fields.BigIntField()
    accepted_answer: fields.ReverseRelation["PostAnswer"]

    posts: fields.ReverseRelation["PostAnswer"]
    tags: fields.ManyToManyRelation["Tag"] = fields.ManyToManyField(
        "models.Tag", related_name="posts", through="tag_to_posts"
    )


class PostAnswer(Model):
    """Answers for posts"""
    id = fields.BigIntField(pk=True)
    post_id = fields.BigIntField()
    start_bytes = fields.BigIntField()
    length = fields.IntField()

    author: fields.ForeignKeyRelation[Post] = fields.ForeignKeyField(
        "models.Post", related_name="posts"
    )

    accepted_post: fields.ForeignKeyRelation[Post] = fields.ForeignKeyField(
        "models.Post", related_name="accepted_answer", to_field="accepted_answer_id"
    )


class Tag(Model):
    """Tag for posts"""
    id = fields.BigIntField(pk=True)
    name = fields.TextField(unique=True)
    count_usage = fields.IntField()
    posts: fields.ManyToManyRelation[Post]
