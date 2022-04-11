"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from threading import Lock

class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size = queue_size_per_producer
        self.producers_lock = Lock()
        self.next_producer_id = 0

        self.carts = []
        self.product_origins = []
        self.carts_lock = Lock()
        self.next_cart_id = 0

        self.market = {}

        self.printing_lock = Lock()

        


    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        producer_id = self.next_producer_id
        with self.producers_lock:
            self.next_producer_id += 1
            self.market[producer_id] = []
        return producer_id

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        if len(self.market[producer_id]) >= self.queue_size:
            return False

        self.market[producer_id].append(product)
        return True
        

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        cart_id = self.next_cart_id
        self.carts.append([])
        self.product_origins.append([])
        with self.carts_lock:
            self.next_cart_id += 1
        return cart_id

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        for producer in self.market:
            for curr_prod in self.market[producer]:
                if curr_prod == product:
                    self.carts[cart_id].append(curr_prod)
                    self.product_origins[cart_id].append(producer)
                    self.market[producer].remove(curr_prod)
                    return True
        return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        for curr_prod, producer in zip(self.carts[cart_id], self.product_origins[cart_id]):
            if curr_prod == product:
                self.market[producer].append(curr_prod)
                self.carts[cart_id].remove(curr_prod)
                self.product_origins[cart_id].remove(producer)
                return


    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        #return list(map(lambda item: item[0], self.carts[cart_id]))
        '''products = []
        for (currProd, _) in self.carts[cart_id]:
            products.append(currProd)
        return products'''
        return self.carts[cart_id]

    def print_order(self, consumer_name, products):
        with self.printing_lock:
            for curr_prod in products:
                print(consumer_name + ' bought ' + str(curr_prod))
